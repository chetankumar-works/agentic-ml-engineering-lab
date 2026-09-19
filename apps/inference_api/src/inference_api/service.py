"""The prediction itself, independent of HTTP: validate → (fetch) →
predict → persist → publish. `api.py` only maps HTTP to this."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from amel_common.schemas import HIGGS_FEATURE_NAMES, PredictionEvent
from amel_db.models import Prediction

from inference_api import metrics
from inference_api.features import FeastOnlineClient
from inference_api.model import ModelCache
from inference_api.publisher import PredictionPublisher
from inference_api.store import PredictionStore


class ModelNotLoaded(RuntimeError):
    pass


class FeaturesNotFound(LookupError):
    pass


@dataclass(frozen=True)
class PredictionResult:
    prediction_id: str
    entity_id: str | None
    source: str
    prediction: int
    probability: float
    model: dict[str, Any]
    latency_ms: float
    trace_id: str
    persisted: bool


class PredictionService:
    def __init__(
        self,
        cache: ModelCache,
        features: FeastOnlineClient | None,
        store: PredictionStore | None,
        publisher: PredictionPublisher | None,
    ) -> None:
        self.cache = cache
        self.features = features
        self.store = store
        self.publisher = publisher

    def predict_raw(
        self, features: dict[str, float], entity_id: str | None, trace_id: str
    ) -> PredictionResult:
        return self._predict(features, entity_id, "raw", trace_id, time.perf_counter())

    def predict_entity(self, entity_id: str, trace_id: str) -> PredictionResult:
        started = time.perf_counter()
        if self.features is None:
            raise RuntimeError("no feature client configured")
        with metrics.FEATURE_FETCH_LATENCY.time():
            feats = self.features.get_features(entity_id)
        if feats is None:
            metrics.PREDICTIONS.labels(source="entity", outcome="no_features").inc()
            raise FeaturesNotFound(entity_id)
        return self._predict(feats, entity_id, "entity", trace_id, started)

    def _predict(
        self,
        features: dict[str, float],
        entity_id: str | None,
        source: str,
        trace_id: str,
        started: float,
    ) -> PredictionResult:
        loaded = self.cache.current
        if loaded is None:
            metrics.PREDICTIONS.labels(source=source, outcome="error").inc()
            raise ModelNotLoaded("no champion model loaded")
        # Column order must match the training signature exactly.
        frame = pd.DataFrame(
            [[features[name] for name in HIGGS_FEATURE_NAMES]], columns=list(HIGGS_FEATURE_NAMES)
        ).astype("float32")
        proba = float(loaded.model.predict_proba(frame)[0, 1])
        label = int(proba >= 0.5)
        latency_ms = (time.perf_counter() - started) * 1000.0
        prediction_id = str(uuid.uuid4())
        meta = loaded.metadata

        persisted = False
        if self.store is not None:
            persisted = self.store.save(
                Prediction(
                    prediction_id=prediction_id,
                    entity_id=entity_id,
                    source=source,
                    model_name=meta.name,
                    model_version=meta.version,
                    model_run_id=meta.run_id,
                    features=features,
                    prediction=label,
                    probability=proba,
                    latency_ms=latency_ms,
                    trace_id=trace_id,
                )
            )
            if not persisted:
                metrics.PERSIST_FAILURES.inc()
        if self.publisher is not None:
            try:
                self.publisher.publish(
                    PredictionEvent(
                        prediction_id=prediction_id,
                        entity_id=entity_id,
                        source=source,
                        model_name=meta.name,
                        model_version=meta.version,
                        model_run_id=meta.run_id,
                        prediction=label,
                        probability=proba,
                        predicted_at=datetime.now(UTC),
                        trace_id=trace_id,
                    )
                )
            except Exception:  # noqa: BLE001 — best-effort; counted
                metrics.PUBLISH_FAILURES.inc()
        metrics.PREDICTIONS.labels(source=source, outcome="ok").inc()
        metrics.PREDICTION_LATENCY.labels(source=source).observe(latency_ms / 1000.0)
        return PredictionResult(
            prediction_id=prediction_id,
            entity_id=entity_id,
            source=source,
            prediction=label,
            probability=proba,
            model=meta.as_dict(),
            latency_ms=round(latency_ms, 3),
            trace_id=trace_id,
            persisted=persisted,
        )
