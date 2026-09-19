from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """`INFERENCE_*` environment variables (see .env.example)."""

    model_config = SettingsConfigDict(env_prefix="INFERENCE_", case_sensitive=False)

    mlflow_tracking_uri: str = "http://mlflow:5000"
    model_name: str = "higgs_decision_tree"
    model_alias: str = "champion"
    # 0 = refresh only via POST /model/refresh; >0 = also poll the alias
    # every N seconds and swap if it moved to a new version.
    model_refresh_seconds: int = 0
    # Required header value for POST /model/refresh. JWT scopes replace
    # this when the platform API lands (Milestone 11); until then a
    # shared secret keeps the one mutating endpoint from being anonymous.
    admin_token: str = "dev-admin-token"

    feast_server_url: str = "http://feast-server:6566"
    feature_view: str = "higgs_features"
    feast_timeout_seconds: float = 2.0

    kafka_bootstrap_servers: str = "kafka:9092"
    predictions_topic: str = "predictions.v1"

    http_port: int = 8003
    log_level: str = "info"
    service_name: str = "inference_api"
