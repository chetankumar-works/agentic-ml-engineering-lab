from __future__ import annotations

from prometheus_client import Counter, Histogram

MESSAGES_CONSUMED = Counter(
    "amel_ingestor_messages_total",
    "Kafka messages consumed, by topic and outcome",
    ["topic", "outcome"],  # outcome: persisted | duplicate_skipped | dlq
)
DB_WRITE_FAILURES = Counter(
    "amel_ingestor_db_write_failures_total",
    "Batch DB write attempts that raised an exception (before a successful retry, if any)",
)
OFFSET_COMMITS = Counter(
    "amel_ingestor_offset_commits_total",
    "Successful Kafka offset commit calls",
)
BATCH_SIZE = Histogram(
    "amel_ingestor_batch_size",
    "Number of Kafka messages processed per batch",
    buckets=(1, 5, 25, 100, 250, 500, 1000, 2500),
)
DB_WRITE_DURATION = Histogram(
    "amel_ingestor_db_write_duration_seconds",
    "Time spent writing one batch to PostgreSQL",
)
