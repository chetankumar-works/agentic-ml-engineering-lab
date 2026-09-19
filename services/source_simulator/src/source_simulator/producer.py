from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from amel_common.logging import get_logger
from confluent_kafka import KafkaError, Producer

logger = get_logger(component="producer")

DeliveryCallback = Callable[[str, "KafkaError | None"], None]


class KafkaEventProducer:
    """Thin wrapper around `confluent_kafka.Producer`. `produce()` itself
    is non-blocking (librdkafka enqueues internally); a dedicated
    background thread calls `poll()` continuously so delivery report
    callbacks actually fire and the internal queue drains.

    Every delivery report — success or failure — is forwarded to
    `on_delivery` so the owner (SimulatorRunner) can turn "Kafka is not
    accepting our writes" into a readiness failure instead of a log line
    nobody reads (the Milestone 3 producer-wedge incident, RUNBOOKS.md).
    """

    def __init__(
        self,
        bootstrap_servers: str,
        message_timeout_ms: int = 30_000,
        on_delivery: DeliveryCallback | None = None,
    ) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 5,
                "linger.ms": 20,
                "message.timeout.ms": message_timeout_ms,
            }
        )
        self.on_delivery = on_delivery
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="kafka-producer-poll"
        )
        self._poll_thread.start()

    @property
    def poll_thread_alive(self) -> bool:
        return self._poll_thread.is_alive()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.2)

    def _delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("kafka_delivery_failed", topic=msg.topic(), error=str(err))
        if self.on_delivery:
            self.on_delivery(msg.topic(), err)

    def publish(self, topic: str, key: str, value: bytes) -> None:
        """Enqueue one message. Raises (BufferError if the local queue is
        still full after a flush, KafkaException on a fatal producer
        state) rather than dropping silently — the caller decides."""
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                on_delivery=self._delivery_report,
            )
        except BufferError:
            logger.warning("kafka_local_queue_full_flushing", topic=topic)
            self._producer.flush(5.0)
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                on_delivery=self._delivery_report,
            )

    def close(self) -> None:
        self._stop.set()
        self._producer.flush(10.0)
        self._poll_thread.join(timeout=2.0)
