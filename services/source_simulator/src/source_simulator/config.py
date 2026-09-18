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

    events_per_second: float = 100.0
    duplicate_rate: float = 0.01
    invalid_event_rate: float = 0.002
    label_delay_seconds: float = 10.0

    burst_mode: bool = False
    burst_interval_seconds: float = 30.0
    burst_duration_seconds: float = 5.0
    burst_rate_multiplier: float = 10.0

    random_seed: int = 42
    log_level: str = "info"
    http_port: int = 8001
    service_name: str = "source_simulator"
