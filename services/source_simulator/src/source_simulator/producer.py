from __future__ import annotations

import threading
from typing import Any

from amel_common.logging import get_logger
from confluent_kafka import Producer

logger = get_logger(component="producer")


class KafkaEventProducer:
    """Thin wrapper around `confluent_kafka.Producer`. `produce()` itself
    is non-blocking (librdkafka enqueues internally); a dedicated
    background thread calls `poll()` continuously so delivery report
    callbacks actually fire and the internal queue drains.
    """

    def __init__(self, bootstrap_servers: str, on_delivery_error: Any = None) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 5,
                "linger.ms": 20,
            }
        )
        self._on_delivery_error = on_delivery_error
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="kafka-producer-poll"
        )
        self._poll_thread.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.2)

    def _delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("kafka_delivery_failed", topic=msg.topic(), error=str(err))
            if self._on_delivery_error:
                self._on_delivery_error(msg.topic(), err)

    def publish(self, topic: str, key: str, value: bytes) -> None:
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
