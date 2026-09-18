from __future__ import annotations

import uvicorn
from amel_common.logging import configure_logging, get_logger

from stream_ingestor.api import create_app
from stream_ingestor.config import Settings
from stream_ingestor.consumer import StreamIngestor
from stream_ingestor.dlq import DlqProducer


def main() -> None:
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)
    logger = get_logger(component="main")
    logger.info("stream_ingestor_booting", kafka=settings.kafka_bootstrap_servers)

    dlq = DlqProducer(settings.kafka_bootstrap_servers)
    ingestor = StreamIngestor(settings, dlq)
    app = create_app(ingestor)

    uvicorn.run(app, host="0.0.0.0", port=settings.http_port, log_config=None)  # noqa: S104


if __name__ == "__main__":
    main()
