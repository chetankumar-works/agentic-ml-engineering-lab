from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from source_simulator.simulator import SimulatorRunner


def create_app(runner: SimulatorRunner) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):  # noqa: ANN001
        runner.start()
        yield
        runner.stop()

    app = FastAPI(title="AMEL source simulator", lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    def ready() -> Response:
        if runner.ready:
            return PlainTextResponse("ready", status_code=200)
        return PlainTextResponse("not ready: dataset not yet available", status_code=503)

    @app.get("/status")
    def status() -> dict[str, object]:
        return {
            "paused": runner.paused,
            "ready": runner.ready,
            "counters": runner.counters.snapshot(),
            "settings": {
                "events_per_second": runner.settings.events_per_second,
                "duplicate_rate": runner.settings.duplicate_rate,
                "invalid_event_rate": runner.settings.invalid_event_rate,
                "label_delay_seconds": runner.settings.label_delay_seconds,
                "burst_mode": runner.settings.burst_mode,
                "higgs_max_rows": runner.settings.higgs_max_rows,
            },
        }

    @app.post("/pause")
    def pause() -> dict[str, bool]:
        runner.pause()
        return {"paused": True}

    @app.post("/resume")
    def resume() -> dict[str, bool]:
        runner.resume()
        return {"paused": False}

    @app.get("/metrics")
    def metrics_endpoint() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
