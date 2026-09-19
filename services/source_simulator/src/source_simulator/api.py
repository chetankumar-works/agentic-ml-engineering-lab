from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from source_simulator import metrics
from source_simulator.simulator import SimulatorRunner


def create_app(runner: SimulatorRunner) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):  # noqa: ANN001
        runner.start()
        yield
        runner.stop()

    app = FastAPI(title="AMEL source simulator", lifespan=lifespan)

    # Liveness: "restart me" — false only when the producer is fatally
    # broken or the publishing loop is dead. Readiness: "don't route/count
    # on me right now" — also false while Kafka is rejecting or timing out
    # our writes. Milestone 8's Kubernetes probes point at exactly these
    # two paths, so they must not lie (RUNBOOKS.md: producer wedge).
    @app.get("/health")
    def health() -> Response:
        live, reason = runner.liveness()
        return PlainTextResponse(reason, status_code=200 if live else 503)

    @app.get("/ready")
    def ready() -> Response:
        ok, reason = runner.readiness()
        metrics.READY.set(1 if ok else 0)
        return PlainTextResponse(reason, status_code=200 if ok else 503)

    @app.get("/status")
    def status() -> dict[str, object]:
        live, live_reason = runner.liveness()
        ok, ready_reason = runner.readiness()
        return {
            "paused": runner.paused,
            "ready": ok,
            "live": live,
            "probe_reasons": {"health": live_reason, "ready": ready_reason},
            "producer_health": runner.health.snapshot(),
            "counters": runner.counters.snapshot(),
            "settings": {
                "events_per_second": runner.settings.events_per_second,
                "duplicate_rate": runner.settings.duplicate_rate,
                "invalid_event_rate": runner.settings.invalid_event_rate,
                "label_delay_seconds": runner.settings.label_delay_seconds,
                "burst_mode": runner.settings.burst_mode,
                "higgs_max_rows": runner.settings.higgs_max_rows,
                "higgs_start_index": runner.settings.higgs_start_index,
                "kafka_message_timeout_ms": runner.settings.kafka_message_timeout_ms,
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
