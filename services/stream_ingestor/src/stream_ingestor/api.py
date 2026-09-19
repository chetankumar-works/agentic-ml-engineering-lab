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

    # Probe contract in consumer.py's module docstring. Both must be
    # honest: Milestone 8's Kubernetes probes inherit whatever these say.
    @app.get("/health")
    def health() -> Response:
        live, reason = ingestor.liveness()
        return PlainTextResponse(reason, status_code=200 if live else 503)

    @app.get("/ready")
    def ready() -> Response:
        ok, reason = ingestor.readiness()
        return PlainTextResponse(reason, status_code=200 if ok else 503)

    @app.get("/status")
    def status() -> dict[str, object]:
        live, live_reason = ingestor.liveness()
        ok, ready_reason = ingestor.readiness()
        return {
            "live": live,
            "ready": ok,
            "probe_reasons": {"health": live_reason, "ready": ready_reason},
            "assigned": ingestor.assigned,
            "commit_failures": ingestor.commit_failures,
            "last_commit_error": ingestor.last_commit_error,
            "loop_error": ingestor.loop_error,
        }

    @app.get("/metrics")
    def metrics_endpoint() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
