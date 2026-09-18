from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from stream_ingestor.consumer import StreamIngestor


def create_app(ingestor: StreamIngestor) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):  # noqa: ANN001
        ingestor.start()
        yield
        ingestor.stop()

    app = FastAPI(title="AMEL stream ingestor", lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    def ready() -> Response:
        if ingestor.ready:
            return PlainTextResponse("ready", status_code=200)
        return PlainTextResponse("not ready: no partition assignment yet", status_code=503)

    @app.get("/metrics")
    def metrics_endpoint() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
