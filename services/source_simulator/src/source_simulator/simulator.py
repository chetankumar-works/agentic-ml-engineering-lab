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

    def snapshot(self) -> dict[str, int]:
        with self.lock:
            return {
                "rows_read": self.rows_read,
                "features_published": self.features_published,
                "labels_published": self.labels_published,
                "duplicates_published": self.duplicates_published,
                "malformed_published": self.malformed_published,
            }


class SimulatorRunner:
    """Owns the background thread that reads HIGGS rows and turns them
    into Kafka traffic. HTTP concerns (FastAPI) only ever touch this
    through the thread-safe methods/properties below.
    """

    def __init__(self, settings: Settings, producer: KafkaEventProducer) -> None:
        self.settings = settings
        self.producer = producer
        self.counters = Counters()
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
        s = self.settings
        zip_path = ensure_dataset(s.data_dir, s.higgs_dataset_url)
        self.ready_event.set()
        logger.info(
            "simulator_started",
            max_rows=s.higgs_max_rows,
            events_per_second=s.events_per_second,
            duplicate_rate=s.duplicate_rate,
            invalid_event_rate=s.invalid_event_rate,
            label_delay_seconds=s.label_delay_seconds,
        )

        for index, row in enumerate(stream_rows(zip_path, s.higgs_max_rows, cycle=True)):
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
            self._publish_feature(entity_id, row.features)
            self._schedule_label(entity_id, row.target)

            if rate > 0:
                time.sleep(1.0 / rate)

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
            self._send(s.labels_topic, entity_id, payload, event_type="label", malformed=malformed)
            if duplicate:
                self._send(
                    s.labels_topic,
                    entity_id,
                    payload,
                    event_type="label",
                    malformed=malformed,
                    duplicate=True,
                )

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
