from __future__ import annotations

import uvicorn
from amel_common.logging import configure_logging, get_logger

from source_simulator.api import create_app
from source_simulator.config import Settings
from source_simulator.producer import KafkaEventProducer
from source_simulator.simulator import SimulatorRunner


def main() -> None:
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)
    logger = get_logger(component="main")
    logger.info("source_simulator_booting", kafka=settings.kafka_bootstrap_servers)

    producer = KafkaEventProducer(settings.kafka_bootstrap_servers)
    runner = SimulatorRunner(settings, producer)
    app = create_app(runner)

    uvicorn.run(app, host="0.0.0.0", port=settings.http_port, log_config=None)  # noqa: S104


if __name__ == "__main__":
    main()
