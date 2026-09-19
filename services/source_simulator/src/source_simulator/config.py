from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_HIGGS_URL = "https://archive.ics.uci.edu/static/public/280/higgs.zip"


class Settings(BaseSettings):
    """All behavior is environment-variable controlled per
    AMEL_KICKOFF_PROMPT.md's Source Simulation section — see .env.example
    for the full list with descriptions.
    """

    model_config = SettingsConfigDict(case_sensitive=False)

    kafka_bootstrap_servers: str = "localhost:9092"
    features_topic: str = "higgs.features.v1"
    labels_topic: str = "higgs.labels.v1"

    data_dir: Path = Path("/data")
    higgs_dataset_url: str = DEFAULT_HIGGS_URL
    higgs_max_rows: int = 500_000
    # entity_id numbering starts here. Every restart otherwise replays
    # `higgs-000000000...`, which the ingestor dedups as already-seen —
    # after a restart set this to (max index already landed + 1) or the
    # pipeline sees no new data for hours (RUNBOOKS.md).
    higgs_start_index: int = 0

    events_per_second: float = 100.0
    duplicate_rate: float = 0.01
    invalid_event_rate: float = 0.002
    label_delay_seconds: float = 10.0

    burst_mode: bool = False
    burst_interval_seconds: float = 30.0
    burst_duration_seconds: float = 5.0
    burst_rate_multiplier: float = 10.0

    # librdkafka's default is 300 s; a dead/paused broker should surface
    # in readiness within tens of seconds, not five minutes.
    kafka_message_timeout_ms: int = 30_000
    # A delivery error makes /ready fail until either a later delivery
    # succeeds or this many seconds pass without another error.
    readiness_error_window_seconds: float = 30.0

    random_seed: int = 42
    log_level: str = "info"
    http_port: int = 8001
    service_name: str = "source_simulator"
