"""Publishes `PredictionEvent`s to `predictions.v1`. Best-effort from the
request's point of view (the row in `ml.predictions` is the record);
failures are counted and logged, and surface through /ready when the
broker is rejecting writes — the Milestone 3 probe lesson applied here
from day one."""

from __future__ import annotations

import threading
import time
from typing import Any

from amel_common.logging import get_logger
from amel_common.schemas import PredictionEvent
from confluent_kafka import Producer

from amel_common import telemetry

logger = get_logger(component="publisher")


class PredictionPublisher:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 10,
                "message.timeout.ms": 30_000,
            }
        )
        # traceparent goes into the message headers, so a consumer of
        # predictions.v1 joins the same trace as the /predict request.
        self._producer = telemetry.instrument_kafka_producer(self._producer)
        self.delivery_failures = 0
        self.last_delivery_error: str | None = None
        self.last_delivery_error_at: float | None = None
        self.last_delivery_ok_at: float | None = None
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True, name="kafka-poll")
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.2)

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is None:
            self.last_delivery_ok_at = time.monotonic()
            return
        self.delivery_failures += 1
        self.last_delivery_error = str(err)
        self.last_delivery_error_at = time.monotonic()
        logger.error("prediction_delivery_failed", error=str(err))

    def publish(self, event: PredictionEvent) -> None:
        self._producer.produce(
            self.topic,
            key=(event.entity_id or event.prediction_id).encode(),
            value=event.model_dump_json().encode(),
            on_delivery=self._on_delivery,
        )

    def degraded(self, window_seconds: float = 30.0) -> str | None:
        err_at = self.last_delivery_error_at
        if err_at is None:
            return None
        recovered = self.last_delivery_ok_at is not None and self.last_delivery_ok_at > err_at
        if recovered or time.monotonic() - err_at > window_seconds:
            return None
        return f"kafka delivery failing: {self.last_delivery_error}"

    def close(self) -> None:
        self._stop.set()
        self._producer.flush(5.0)
