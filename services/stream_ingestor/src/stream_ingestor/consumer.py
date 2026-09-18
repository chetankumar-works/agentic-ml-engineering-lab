"""The stream ingestor's core loop.

Delivery/consistency model (see LEARNING_LOG.md Milestone 1 entry for the
full writeup): Kafka gives at-least-once delivery (a message can be
redelivered after a crash between processing and offset commit). This
loop makes that safe by:

1. Never committing an offset until the DB transaction that persisted the
   corresponding rows has actually committed.
2. Making the DB write idempotent (`INSERT ... ON CONFLICT (event_id) DO
   NOTHING`), so redelivering an already-persisted event is a no-op, not
   a duplicate row.
3. On a DB write that keeps failing after retries, stopping the process
   instead of silently dropping the batch or committing offsets anyway —
   because offsets were never committed, a restart naturally redelivers
   the same messages once the DB is healthy again.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any

from amel_common.logging import get_logger
from amel_common.schemas import FeatureEvent, LabelEvent
from amel_db.models import HiggsFeatureEvent, HiggsLabelEvent
from amel_db.session import session_scope
from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert as pg_insert

from stream_ingestor import metrics
from stream_ingestor.config import Settings
from stream_ingestor.dlq import DlqProducer

logger = get_logger(component="consumer")


class StreamIngestor:
    def __init__(self, settings: Settings, dlq: DlqProducer) -> None:
        self.settings = settings
        self.dlq = dlq
        self.ready = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe(
            [settings.features_topic, settings.labels_topic],
            on_assign=self._on_assign,
        )

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        logger.info(
            "consumer_partitions_assigned",
            partitions=[f"{p.topic}[{p.partition}]" for p in partitions],
        )
        self.ready = True

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="stream-ingestor-loop")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.settings.batch_timeout_seconds + 10.0)
        self.dlq.flush()

    def _run(self) -> None:
        logger.info(
            "stream_ingestor_started",
            topics=[self.settings.features_topic, self.settings.labels_topic],
            group=self.settings.consumer_group,
        )
        try:
            while not self._stop.is_set():
                batch = self._poll_batch()
                if batch:
                    self._process_batch(batch)
        finally:
            self._consumer.close()
            logger.info("stream_ingestor_stopped")

    def _poll_batch(self) -> list[Message]:
        batch: list[Message] = []
        deadline = time.monotonic() + self.settings.batch_timeout_seconds
        while len(batch) < self.settings.batch_size and time.monotonic() < deadline:
            if self._stop.is_set():
                break
            msg = self._consumer.poll(timeout=0.5)
            if msg is None:
                continue
            error = msg.error()
            if error is not None:
                if error.code() != KafkaError._PARTITION_EOF:
                    logger.error("kafka_consume_error", error=str(error))
                continue
            batch.append(msg)
        return batch

    def _process_batch(self, batch: list[Message]) -> None:
        metrics.BATCH_SIZE.observe(len(batch))
        features_rows: list[dict[str, Any]] = []
        labels_rows: list[dict[str, Any]] = []
        max_offset: dict[tuple[str, int], int] = {}

        for msg in batch:
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            if topic is None or partition is None or offset is None:
                logger.error("kafka_message_missing_metadata")
                continue
            key = (topic, partition)
            max_offset[key] = max(max_offset.get(key, -1), offset)

            raw = msg.value()
            parsed, error_reason = self._validate(topic, raw)
            if parsed is None:
                self.dlq.send(
                    self.settings.dlq_topic_for(topic),
                    raw or b"",
                    original_topic=topic,
                    partition=partition,
                    offset=offset,
                    reason=error_reason or "unknown_validation_error",
                )
                metrics.MESSAGES_CONSUMED.labels(topic=topic, outcome="dlq").inc()
                continue

            row = parsed.model_dump(mode="json")
            row["kafka_partition"] = partition
            row["kafka_offset"] = offset
            if topic == self.settings.features_topic:
                features_rows.append(row)
            else:
                labels_rows.append(row)

        self._write_with_retry(features_rows, labels_rows)

        offsets = [
            TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in max_offset.items()
        ]
        self._consumer.commit(offsets=offsets, asynchronous=False)
        metrics.OFFSET_COMMITS.inc()

    def _validate(
        self, topic: str, raw: bytes | None
    ) -> tuple[FeatureEvent | LabelEvent | None, str | None]:
        if raw is None:
            return None, "empty_message_value"
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            return None, f"invalid_json: {exc}"

        model = FeatureEvent if topic == self.settings.features_topic else LabelEvent
        try:
            return model.model_validate(data), None
        except ValidationError as exc:
            return None, f"schema_validation_failed: {exc.error_count()} error(s)"

    def _write_with_retry(
        self, features_rows: list[dict[str, Any]], labels_rows: list[dict[str, Any]]
    ) -> None:
        if not features_rows and not labels_rows:
            return
        attempt = 0
        while True:
            try:
                start = time.monotonic()
                self._write_batch(features_rows, labels_rows)
                metrics.DB_WRITE_DURATION.observe(time.monotonic() - start)
                return
            except Exception:
                attempt += 1
                metrics.DB_WRITE_FAILURES.inc()
                if attempt > self.settings.max_retries:
                    logger.critical(
                        "db_write_exhausted_retries_stopping_ingestor",
                        attempts=attempt,
                        exc_info=True,
                    )
                    raise
                backoff = self.settings.retry_backoff_base_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "db_write_failed_retrying",
                    attempt=attempt,
                    backoff_seconds=backoff,
                    exc_info=True,
                )
                time.sleep(backoff)

    def _write_batch(
        self, features_rows: list[dict[str, Any]], labels_rows: list[dict[str, Any]]
    ) -> None:
        with session_scope() as session:
            for rows, orm_model, topic in (
                (features_rows, HiggsFeatureEvent, self.settings.features_topic),
                (labels_rows, HiggsLabelEvent, self.settings.labels_topic),
            ):
                if not rows:
                    continue
                stmt = (
                    pg_insert(orm_model)
                    .values(rows)
                    .on_conflict_do_nothing(index_elements=["event_id"])
                    .returning(orm_model.event_id)
                )
                inserted = session.execute(stmt).fetchall()
                skipped = len(rows) - len(inserted)
                metrics.MESSAGES_CONSUMED.labels(topic=topic, outcome="persisted").inc(
                    len(inserted)
                )
                if skipped:
                    metrics.MESSAGES_CONSUMED.labels(topic=topic, outcome="duplicate_skipped").inc(
                        skipped
                    )
                    logger.info("duplicate_events_skipped", topic=topic, count=skipped)
