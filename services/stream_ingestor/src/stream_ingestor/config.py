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

    batch_size: int = 500
    batch_timeout_seconds: float = 2.0
    max_retries: int = 5
    retry_backoff_base_seconds: float = 0.5

    log_level: str = "info"
    http_port: int = 8002
    service_name: str = "stream_ingestor"

    def dlq_topic_for(self, topic: str) -> str:
        if topic == self.features_topic:
            return self.features_dlq_topic
        if topic == self.labels_topic:
            return self.labels_dlq_topic
        raise ValueError(f"no DLQ topic configured for {topic!r}")
