from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    kafka_bootstrap_servers: str = "localhost:9092"
    features_topic: str = "higgs.features.v1"
    labels_topic: str = "higgs.labels.v1"
    features_dlq_topic: str = "higgs.features.dlq"
    labels_dlq_topic: str = "higgs.labels.dlq"
    consumer_group: str = "stream-ingestor"
    # librdkafka prefetch bounds, per consumer. Defaults are librdkafka's
    # own (2.15.1): a 64 MiB pre-fetch queue plus up to 50 MiB per fetch
    # response — under backlog that is ~115 MiB per replica on top of the
    # steady ~86 MiB. The k8s Deployment lowers both (DECISIONS.md ADR-0012).
    kafka_queued_max_messages_kbytes: int = 65536
    kafka_fetch_max_bytes: int = 52428800

    batch_size: int = 500
    batch_timeout_seconds: float = 2.0
    max_retries: int = 5
    # Liveness fails if the consume loop makes no progress (a completed
    # poll cycle or batch) for this long. A Postgres that is frozen rather
    # than down makes the DB call hang forever with no error to retry —
    # found in Milestone 8's probe test. Must exceed batch_timeout +
    # the full DB retry budget (~16 s at the defaults).
    stall_timeout_seconds: float = 120.0
    retry_backoff_base_seconds: float = 0.5

    log_level: str = "info"
    http_port: int = 8002
    service_name: str = "stream_ingestor"

    def consumer_config(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "queued.max.messages.kbytes": self.kafka_queued_max_messages_kbytes,
            "fetch.max.bytes": self.kafka_fetch_max_bytes,
        }

    def dlq_topic_for(self, topic: str) -> str:
        if topic == self.features_topic:
            return self.features_dlq_topic
        if topic == self.labels_topic:
            return self.labels_dlq_topic
        raise ValueError(f"no DLQ topic configured for {topic!r}")
