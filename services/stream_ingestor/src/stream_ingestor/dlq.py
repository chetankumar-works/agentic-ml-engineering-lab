"""Dead-letter publishing. A message that fails JSON parsing or schema
validation is not dropped and does not block the batch — it's wrapped in
an envelope (original topic/partition/offset, the reason, and the raw
bytes base64-encoded so binary-unsafe/invalid-UTF8 payloads survive
intact) and published to the corresponding `.dlq` topic. Its offset is
then committed normally: routing to the DLQ *is* how that message was
handled, so it must not be redelivered forever.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from typing import Any

from amel_common.logging import get_logger
from confluent_kafka import Producer

logger = get_logger(component="dlq")


class DlqProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer(
            {"bootstrap.servers": bootstrap_servers, "enable.idempotence": True}
        )

    def send(
        self,
        dlq_topic: str,
        raw_value: bytes,
        *,
        original_topic: str,
        partition: int,
        offset: int,
        reason: str,
    ) -> None:
        envelope = {
            "original_topic": original_topic,
            "original_partition": partition,
            "original_offset": offset,
            "reason": reason,
            "raw_value_base64": base64.b64encode(raw_value).decode("ascii"),
            "failed_at": datetime.now(UTC).isoformat(),
        }
        self._producer.produce(
            dlq_topic,
            value=json.dumps(envelope).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("dlq_delivery_failed", topic=msg.topic(), error=str(err))

    def flush(self) -> None:
        self._producer.flush(10.0)
