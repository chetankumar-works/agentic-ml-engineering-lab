from __future__ import annotations

import uvicorn
from amel_common.logging import configure_logging, get_logger

from inference_api.api import create_app
from inference_api.config import Settings
from inference_api.features import FeastOnlineClient
from inference_api.model import ModelCache, mlflow_loader
from inference_api.publisher import PredictionPublisher
from inference_api.service import PredictionService
from inference_api.store import PredictionStore


def main() -> None:
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)
    get_logger(component="main").info(
        "inference_api_booting",
        model=f"models:/{settings.model_name}@{settings.model_alias}",
        mlflow=settings.mlflow_tracking_uri,
        feast=settings.feast_server_url,
    )
    service = PredictionService(
        cache=ModelCache(
            settings.model_name, settings.model_alias, mlflow_loader(settings.mlflow_tracking_uri)
        ),
        features=FeastOnlineClient(
            settings.feast_server_url, settings.feature_view, settings.feast_timeout_seconds
        ),
        store=PredictionStore(),
        publisher=PredictionPublisher(settings.kafka_bootstrap_servers, settings.predictions_topic),
    )
    uvicorn.run(
        create_app(service, settings), host="0.0.0.0", port=settings.http_port, log_config=None
    )  # noqa: S104


if __name__ == "__main__":
    main()
