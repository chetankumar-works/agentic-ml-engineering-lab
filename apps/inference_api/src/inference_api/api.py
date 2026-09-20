from __future__ import annotations

import threading
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Any

from amel_common.logging import get_logger
from amel_common.schemas import HIGGS_FEATURE_NAMES
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field, field_validator

from amel_common import telemetry
from inference_api import metrics
from inference_api.config import Settings
from inference_api.service import FeaturesNotFound, ModelNotLoaded, PredictionService

logger = get_logger(component="api")
_EXPECTED = frozenset(HIGGS_FEATURE_NAMES)


class RawPredictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    features: dict[str, float]
    entity_id: str | None = Field(default=None, max_length=64)

    @field_validator("features")
    @classmethod
    def exactly_the_higgs_features(cls, v: dict[str, float]) -> dict[str, float]:
        got = frozenset(v)
        if got != _EXPECTED:
            raise ValueError(
                f"features must be exactly the 28 HIGGS fields; "
                f"missing={sorted(_EXPECTED - got)} unexpected={sorted(got - _EXPECTED)}"
            )
        return v


def trace_id(x_trace_id: Annotated[str | None, Header()] = None) -> str:
    """The id every log line, DB row and Kafka event for this request
    carries. Precedence: the active OpenTelemetry trace (a caller's W3C
    `traceparent` or one started by the FastAPI instrumentation) so the
    id in the response is the id in Tempo; else a caller-supplied
    `X-Trace-Id`; else a fresh one. Module-level on purpose: with
    `from __future__ import annotations`, FastAPI resolves annotation
    strings against module globals."""
    otel_trace_id, _ = telemetry.current_trace_ids()
    return otel_trace_id or x_trace_id or uuid.uuid4().hex


class Probes:
    """Liveness: the process can serve or recover (a failed refresh is not
    fatal — the previous model keeps serving). Readiness: a model is
    loaded AND the dependencies a prediction needs right now are healthy."""

    def __init__(self, service: PredictionService, settings: Settings) -> None:
        self.service = service
        self.settings = settings

    def liveness(self) -> tuple[bool, str]:
        return True, "ok"

    def readiness(self) -> tuple[bool, str]:
        cache = self.service.cache
        if not cache.loaded:
            return False, f"no model loaded: {cache.last_error or 'initial load pending'}"
        if self.service.features is not None and not self.service.features.health():
            return False, "feast-server unreachable"
        if self.service.store is not None and not self.service.store.ping():
            return False, f"database unreachable: {self.service.store.last_error}"
        if self.service.publisher is not None:
            degraded = self.service.publisher.degraded()
            if degraded:
                return False, degraded
        return True, "ready"


def create_app(service: PredictionService, settings: Settings) -> FastAPI:
    probes = Probes(service, settings)
    stop = threading.Event()

    def _refresh_loop() -> None:
        while not stop.wait(settings.model_refresh_seconds):
            prev, cur = service.cache.refresh()
            _publish_model_metrics(service)
            if prev != cur:
                logger.info("model_auto_refreshed", previous=prev, current=cur)

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # noqa: ANN001
        service.cache.load_initial()
        _publish_model_metrics(service)
        if settings.model_refresh_seconds > 0:
            threading.Thread(target=_refresh_loop, daemon=True, name="model-refresh").start()
        yield
        stop.set()
        if service.publisher is not None:
            service.publisher.close()
        if service.features is not None:
            service.features.close()

    app = FastAPI(title="AMEL inference API", version="0.1.0", lifespan=lifespan)

    @app.get("/health")
    def health() -> Response:
        live, reason = probes.liveness()
        return PlainTextResponse(reason, status_code=200 if live else 503)

    @app.get("/ready")
    def ready() -> Response:
        ok, reason = probes.readiness()
        metrics.READY.set(1 if ok else 0)
        return PlainTextResponse(reason, status_code=200 if ok else 503)

    @app.get("/model")
    def model() -> dict[str, Any]:
        cache = service.cache
        return {
            "loaded": cache.loaded,
            "model": cache.current.metadata.as_dict() if cache.current else None,
            "alias": f"models:/{cache.name}@{cache.alias}",
            "refresh_policy": (
                f"every {settings.model_refresh_seconds}s + POST /model/refresh"
                if settings.model_refresh_seconds > 0
                else "manual: POST /model/refresh"
            ),
            "refresh_count": cache.refresh_count,
            "last_error": cache.last_error,
        }

    @app.post("/model/refresh")
    def refresh(x_admin_token: Annotated[str | None, Header()] = None) -> dict[str, Any]:
        if x_admin_token != settings.admin_token:
            raise HTTPException(status_code=401, detail="invalid or missing X-Admin-Token")
        previous, current = service.cache.refresh()
        _publish_model_metrics(service)
        return {
            "previous_version": previous,
            "current_version": current,
            "swapped": previous != current,
            "error": service.cache.last_error,
        }

    @app.post("/predict/raw")
    def predict_raw(
        body: RawPredictRequest, tid: Annotated[str, Depends(trace_id)], response: Response
    ) -> dict[str, Any]:
        try:
            result = service.predict_raw(body.features, body.entity_id, tid)
        except ModelNotLoaded as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        response.headers["X-Trace-Id"] = tid
        return result.__dict__

    @app.post("/predict/entity/{entity_id}")
    def predict_entity(
        entity_id: str, tid: Annotated[str, Depends(trace_id)], response: Response
    ) -> dict[str, Any]:
        try:
            result = service.predict_entity(entity_id, tid)
        except FeaturesNotFound:
            raise HTTPException(
                status_code=404, detail=f"no online features for entity {entity_id}"
            ) from None
        except ModelNotLoaded as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:  # feature server errors
            raise HTTPException(status_code=502, detail=f"feature fetch failed: {exc}") from exc
        response.headers["X-Trace-Id"] = tid
        return result.__dict__

    @app.get("/metrics")
    def metrics_endpoint() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.middleware("http")
    async def log_requests(request: Request, call_next):  # noqa: ANN001, ANN202
        response = await call_next(request)
        if request.url.path.startswith("/predict"):
            logger.info(
                "prediction_request",
                path=request.url.path,
                status=response.status_code,
                trace_id=response.headers.get("X-Trace-Id"),
            )
        return response

    return app


def _publish_model_metrics(service: PredictionService) -> None:
    cur = service.cache.current
    metrics.MODEL_LOADED.set(1 if cur else 0)
    if cur:
        metrics.MODEL_INFO.info(
            {
                "name": cur.metadata.name,
                "version": cur.metadata.version,
                "alias": cur.metadata.alias,
            }
        )
        metrics.MODEL_REFRESHES.inc(0)
