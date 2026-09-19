from __future__ import annotations

import json
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from amel_common.logging import get_logger
from amel_common.schemas import FeatureEvent, LabelEvent
from confluent_kafka import KafkaException

from source_simulator import metrics
from source_simulator.config import Settings
from source_simulator.dataset import ensure_dataset, stream_rows
from source_simulator.delay_queue import DelayedDispatcher
from source_simulator.malformed import corrupt_feature_payload, corrupt_label_payload
from source_simulator.producer import KafkaEventProducer

logger = get_logger(component="simulator")


@dataclass
class Counters:
    lock: threading.Lock = field(default_factory=threading.Lock)
    rows_read: int = 0
    features_published: int = 0
    labels_published: int = 0
    duplicates_published: int = 0
    malformed_published: int = 0
    events_dropped: int = 0

    def snapshot(self) -> dict[str, int]:
        with self.lock:
            return {
                "rows_read": self.rows_read,
                "features_published": self.features_published,
                "labels_published": self.labels_published,
                "duplicates_published": self.duplicates_published,
                "malformed_published": self.malformed_published,
                "events_dropped": self.events_dropped,
            }


@dataclass
class ProducerHealth:
    """What the HTTP probes report. Kept as plain timestamps/strings so
    /status can dump it and so the readiness rule is a pure function of
    this state (see `SimulatorRunner.readiness`).

    Two different failure classes, deliberately kept apart:
    - delivery errors (broker unreachable, message timed out): transient,
      self-clearing once deliveries succeed again -> readiness only.
    - fatal errors (librdkafka declares the idempotent producer unusable)
      or the publishing loop thread dying: the process cannot recover
      without a restart -> liveness fails too, which is what makes a
      Kubernetes liveness probe (Milestone 8) restart it.
    """

    lock: threading.Lock = field(default_factory=threading.Lock)
    delivery_failures: int = 0
    last_delivery_error: str | None = None
    last_delivery_error_at: float | None = None
    last_delivery_ok_at: float | None = None
    fatal_error: str | None = None
    loop_error: str | None = None

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "delivery_failures": self.delivery_failures,
                "last_delivery_error": self.last_delivery_error,
                "seconds_since_last_delivery_error": _age(self.last_delivery_error_at),
                "seconds_since_last_delivery_ok": _age(self.last_delivery_ok_at),
                "fatal_error": self.fatal_error,
                "loop_error": self.loop_error,
            }


def _age(t: float | None) -> float | None:
    return None if t is None else round(time.monotonic() - t, 1)


class SimulatorRunner:
    """Owns the background thread that reads HIGGS rows and turns them
    into Kafka traffic. HTTP concerns (FastAPI) only ever touch this
    through the thread-safe methods/properties below.
    """

    def __init__(self, settings: Settings, producer: KafkaEventProducer) -> None:
        self.settings = settings
        self.producer = producer
        self.counters = Counters()
        self.health = ProducerHealth()
        self.rng = random.Random(settings.random_seed)
        self.pause_event = threading.Event()
        self.pause_event.set()  # set == running; cleared == paused
        self.stop_event = threading.Event()
        self.ready_event = threading.Event()
        self.dispatcher = DelayedDispatcher()
        self._thread = threading.Thread(target=self._run, daemon=True, name="simulator-loop")
        self._start_time = time.monotonic()

    @property
    def paused(self) -> bool:
        return not self.pause_event.is_set()

    @property
    def ready(self) -> bool:
        return self.ready_event.is_set()

    def pause(self) -> None:
        self.pause_event.clear()
        metrics.PAUSED.set(1)
        logger.info("simulator_paused")

    def resume(self) -> None:
        self.pause_event.set()
        metrics.PAUSED.set(0)
        logger.info("simulator_resumed")

    # --- probes -----------------------------------------------------------

    def record_delivery(self, topic: str, err: Any) -> None:
        """Called from the producer's poll thread for every delivery report."""
        now = time.monotonic()
        with self.health.lock:
            if err is None:
                self.health.last_delivery_ok_at = now
                return
            self.health.delivery_failures += 1
            self.health.last_delivery_error = str(err)
            self.health.last_delivery_error_at = now
            if _is_fatal(err):
                self.health.fatal_error = str(err)
        metrics.DELIVERY_ERRORS.labels(topic=topic).inc()

    def liveness(self) -> tuple[bool, str]:
        """False only for states a restart is the *only* way out of."""
        h = self.health
        with h.lock:
            if h.fatal_error:
                return False, f"kafka producer fatal error: {h.fatal_error}"
            if h.loop_error:
                return False, f"simulator loop died: {h.loop_error}"
        if (
            self._thread.ident is not None
            and not self._thread.is_alive()
            and not self.stop_event.is_set()
        ):
            return False, "simulator loop thread exited unexpectedly"
        if not self.producer.poll_thread_alive:
            return False, "kafka producer poll thread exited"
        return True, "ok"

    def readiness(self) -> tuple[bool, str]:
        """Liveness plus: dataset available and Kafka currently accepting
        our writes. A delivery error is "current" until a later delivery
        succeeds or `readiness_error_window_seconds` pass without another."""
        live, reason = self.liveness()
        if not live:
            return False, reason
        if not self.ready:
            return False, "dataset not yet available"
        h = self.health
        with h.lock:
            err_at, ok_at, err = (
                h.last_delivery_error_at,
                h.last_delivery_ok_at,
                h.last_delivery_error,
            )
        if err_at is not None:
            recovered = ok_at is not None and ok_at > err_at
            expired = time.monotonic() - err_at > self.settings.readiness_error_window_seconds
            if not (recovered or expired):
                return False, f"kafka delivery failing: {err}"
        return True, "ready"

    # --- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.pause_event.set()  # unblock a paused wait() so the loop can observe stop
        self._thread.join(timeout=5.0)
        self.dispatcher.close()
        self.producer.close()

    def _effective_rate(self) -> float:
        s = self.settings
        if not s.burst_mode:
            return s.events_per_second
        elapsed = time.monotonic() - self._start_time
        cycle = s.burst_interval_seconds + s.burst_duration_seconds
        position = elapsed % cycle if cycle > 0 else 0.0
        if position < s.burst_duration_seconds:
            return s.events_per_second * s.burst_rate_multiplier
        return s.events_per_second

    def _run(self) -> None:
        # Outer guard: an uncaught exception here used to kill this daemon
        # thread silently while /health kept returning 200. Now it is
        # recorded and surfaces through liveness.
        try:
            self._loop()
        except Exception as exc:  # noqa: BLE001 — thread boundary, must record everything
            logger.error("simulator_loop_crashed", error=repr(exc))
            with self.health.lock:
                self.health.loop_error = repr(exc)

    def _loop(self) -> None:
        s = self.settings
        zip_path = ensure_dataset(s.data_dir, s.higgs_dataset_url)
        self.ready_event.set()
        logger.info(
            "simulator_started",
            max_rows=s.higgs_max_rows,
            start_index=s.higgs_start_index,
            events_per_second=s.events_per_second,
            duplicate_rate=s.duplicate_rate,
            invalid_event_rate=s.invalid_event_rate,
            label_delay_seconds=s.label_delay_seconds,
        )

        rows = stream_rows(zip_path, s.higgs_max_rows, cycle=True)
        for index, row in enumerate(rows, start=s.higgs_start_index):
            if self.stop_event.is_set():
                return
            self.pause_event.wait()
            if self.stop_event.is_set():
                return

            with self.counters.lock:
                self.counters.rows_read += 1

            rate = self._effective_rate()
            metrics.CURRENT_RATE.set(rate)
            entity_id = f"higgs-{index:09d}"
            try:
                self._publish_feature(entity_id, row.features)
                self._schedule_label(entity_id, row.target)
            except Exception as exc:  # noqa: BLE001 — see _on_publish_error
                if self._on_publish_error("feature", exc):
                    return
                time.sleep(1.0)  # back off; the event is dropped and counted

            if rate > 0:
                time.sleep(1.0 / rate)

    def _on_publish_error(self, event_type: str, exc: Exception) -> bool:
        """Record a failed hand-off to the producer. Returns True when the
        error is fatal (producer unusable -> stop the loop; liveness
        fails; only a restart helps)."""
        fatal = isinstance(exc, KafkaException) and _is_fatal(exc.args[0])
        logger.error(
            "simulator_publish_failed", event_type=event_type, error=repr(exc), fatal=fatal
        )
        metrics.EVENTS_DROPPED.labels(event_type=event_type).inc()
        with self.counters.lock:
            self.counters.events_dropped += 1
        if fatal:
            with self.health.lock:
                self.health.fatal_error = str(exc.args[0])
        return fatal

    def _publish_feature(self, entity_id: str, features: dict[str, float]) -> None:
        s = self.settings
        event = FeatureEvent(
            event_id=f"feat-{entity_id}",
            entity_id=entity_id,
            event_timestamp=_now(),
            source=s.service_name,
            features=features,
            trace_id=uuid.uuid4().hex,
        )
        payload = event.model_dump(mode="json")
        malformed = self.rng.random() < s.invalid_event_rate
        if malformed:
            payload = corrupt_feature_payload(self.rng, payload)

        self._send(s.features_topic, entity_id, payload, event_type="feature", malformed=malformed)

        if self.rng.random() < s.duplicate_rate:
            self._send(
                s.features_topic,
                entity_id,
                payload,
                event_type="feature",
                malformed=malformed,
                duplicate=True,
            )

    def _schedule_label(self, entity_id: str, target: int) -> None:
        s = self.settings
        event = LabelEvent(
            event_id=f"label-{entity_id}",
            entity_id=entity_id,
            target=target,
            label_timestamp=_now(),
        )
        payload = event.model_dump(mode="json")
        malformed = self.rng.random() < s.invalid_event_rate
        if malformed:
            payload = corrupt_label_payload(self.rng, payload)
        duplicate = self.rng.random() < s.duplicate_rate

        def dispatch() -> None:
            try:
                self._send(
                    s.labels_topic, entity_id, payload, event_type="label", malformed=malformed
                )
                if duplicate:
                    self._send(
                        s.labels_topic,
                        entity_id,
                        payload,
                        event_type="label",
                        malformed=malformed,
                        duplicate=True,
                    )
            except Exception as exc:  # noqa: BLE001 — dispatcher thread boundary
                self._on_publish_error("label", exc)

        self.dispatcher.schedule(s.label_delay_seconds, dispatch)

    def _send(
        self,
        topic: str,
        key: str,
        payload: dict[str, Any],
        *,
        event_type: str,
        malformed: bool,
        duplicate: bool = False,
    ) -> None:
        self.producer.publish(topic, key, json.dumps(payload).encode("utf-8"))

        outcome = "malformed" if malformed else ("duplicate" if duplicate else "ok")
        metrics.EVENTS_PUBLISHED.labels(event_type=event_type, outcome=outcome).inc()
        with self.counters.lock:
            if event_type == "feature":
                self.counters.features_published += 1
            else:
                self.counters.labels_published += 1
            if malformed:
                self.counters.malformed_published += 1
            if duplicate:
                self.counters.duplicates_published += 1


def _now() -> datetime:
    return datetime.now(UTC)


def _is_fatal(err: Any) -> bool:
    fatal = getattr(err, "fatal", None)
    return bool(fatal()) if callable(fatal) else False
