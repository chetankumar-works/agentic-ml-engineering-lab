"""The whole request path with fakes: a real (tiny) sklearn tree, a fake
loader, a fake feature client, a recording publisher, a recording store.
No MLflow, Feast, Kafka or Postgres."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pytest
from amel_common.schemas import HIGGS_FEATURE_NAMES, PredictionEvent
from fastapi.testclient import TestClient
from inference_api.api import RawPredictRequest, create_app
from inference_api.config import Settings
from inference_api.model import LoadedModel, ModelCache, ModelMetadata
from inference_api.service import PredictionService
from sklearn.tree import DecisionTreeClassifier

N = len(HIGGS_FEATURE_NAMES)


def _tree(version: str) -> LoadedModel:
    rng = np.random.default_rng(int(version))
    X = rng.normal(size=(200, N)).astype("float32")
    y = (X[:, 0] > 0).astype(int)
    model = DecisionTreeClassifier(max_depth=3, random_state=0).fit(X, y)
    return LoadedModel(
        model=model,
        metadata=ModelMetadata(
            name="higgs_decision_tree",
            alias="champion",
            version=version,
            run_id=f"run-{version}",
            git_sha="abc",
            dataset_fingerprint="fp",
            feature_view="higgs_features:v1",
            test_accuracy="0.69",
            loaded_at=datetime.now(UTC),
        ),
    )


class FakeLoader:
    def __init__(self) -> None:
        self.version = "3"
        self.fail = False
        self.calls = 0

    def __call__(self, name: str, alias: str) -> LoadedModel:
        self.calls += 1
        if self.fail:
            raise RuntimeError("mlflow down")
        return _tree(self.version)


class FakeFeatures:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, float]] = {}
        self.healthy = True

    def health(self) -> bool:
        return self.healthy

    def get_features(self, entity_id: str) -> dict[str, float] | None:
        return self.rows.get(entity_id)

    def close(self) -> None:
        pass


@dataclass
class FakeStore:
    saved: list[Any] = field(default_factory=list)
    ok: bool = True
    last_error: str | None = None

    def ping(self) -> bool:
        return self.ok

    def save(self, row: Any) -> bool:
        if not self.ok:
            self.last_error = "db down"
            return False
        self.saved.append(row)
        return True


@dataclass
class FakePublisher:
    events: list[PredictionEvent] = field(default_factory=list)
    degraded_reason: str | None = None

    def publish(self, event: PredictionEvent) -> None:
        self.events.append(event)

    def degraded(self) -> str | None:
        return self.degraded_reason

    def close(self) -> None:
        pass


def _features(seed: int = 1) -> dict[str, float]:
    rng = np.random.default_rng(seed)
    return {name: float(v) for name, v in zip(HIGGS_FEATURE_NAMES, rng.normal(size=N), strict=True)}


@pytest.fixture
def stack():  # noqa: ANN201
    loader = FakeLoader()
    cache = ModelCache("higgs_decision_tree", "champion", loader)
    features, store, publisher = FakeFeatures(), FakeStore(), FakePublisher()
    service = PredictionService(cache, features, store, publisher)  # type: ignore[arg-type]
    settings = Settings(admin_token="secret")
    app = create_app(service, settings)
    with TestClient(app) as client:
        yield client, loader, cache, features, store, publisher


def test_raw_request_validation_requires_exactly_28_features() -> None:
    with pytest.raises(ValueError, match="missing="):
        RawPredictRequest(features={"lepton_pt": 1.0})
    ok = RawPredictRequest(features=_features(), entity_id="e1")
    assert ok.entity_id == "e1"


def test_startup_loads_champion_and_serves_raw_predictions(stack) -> None:  # noqa: ANN001
    client, loader, cache, _, store, publisher = stack
    assert loader.calls == 1 and cache.current.metadata.version == "3"
    r = client.post(
        "/predict/raw",
        json={"features": _features(), "entity_id": "e1"},
        headers={"X-Trace-Id": "t-1"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["prediction"] in (0, 1) and 0.0 <= body["probability"] <= 1.0
    assert body["model"]["version"] == "3" and body["model"]["run_id"] == "run-3"
    assert body["trace_id"] == "t-1" and r.headers["X-Trace-Id"] == "t-1"
    assert body["persisted"] is True and len(store.saved) == 1
    assert store.saved[0].prediction_id == body["prediction_id"]
    assert len(publisher.events) == 1 and publisher.events[0].model_version == "3"
    assert publisher.events[0].prediction_id == body["prediction_id"]


def test_entity_prediction_uses_online_features_and_404s_when_absent(stack) -> None:  # noqa: ANN001
    client, _, _, features, _, _ = stack
    assert client.post("/predict/entity/unknown").status_code == 404
    features.rows["higgs-000000001"] = _features(7)
    r = client.post("/predict/entity/higgs-000000001")
    assert r.status_code == 200
    assert r.json()["entity_id"] == "higgs-000000001" and r.json()["source"] == "entity"


def test_model_endpoint_and_authenticated_refresh_swaps_only_on_new_version(stack) -> None:  # noqa: ANN001
    client, loader, cache, _, _, _ = stack
    assert client.get("/model").json()["model"]["version"] == "3"
    assert client.post("/model/refresh").status_code == 401
    same = client.post("/model/refresh", headers={"X-Admin-Token": "secret"}).json()
    assert same == {
        "previous_version": "3",
        "current_version": "3",
        "swapped": False,
        "error": None,
    }
    loader.version = "5"
    moved = client.post("/model/refresh", headers={"X-Admin-Token": "secret"}).json()
    assert moved["swapped"] is True and moved["current_version"] == "5"
    assert (
        client.post("/predict/raw", json={"features": _features()}).json()["model"]["version"]
        == "5"
    )
    assert cache.refresh_count == 2


def test_failed_refresh_keeps_serving_previous_model(stack) -> None:  # noqa: ANN001
    client, loader, cache, _, _, _ = stack
    loader.fail = True
    r = client.post("/model/refresh", headers={"X-Admin-Token": "secret"}).json()
    assert r["swapped"] is False and "mlflow down" in r["error"]
    assert client.post("/predict/raw", json={"features": _features()}).status_code == 200
    assert client.get("/health").status_code == 200
    assert client.get("/ready").status_code == 200, (
        "a failed refresh with a model still loaded is ready"
    )
    loader.fail = False
    recovered = client.post("/model/refresh", headers={"X-Admin-Token": "secret"}).json()
    assert recovered["swapped"] is False and recovered["error"] is None, "stale error must clear"


def test_readiness_reflects_dependencies(stack) -> None:  # noqa: ANN001
    client, _, _, features, store, publisher = stack
    assert client.get("/ready").status_code == 200
    features.healthy = False
    assert client.get("/ready").text == "feast-server unreachable"
    features.healthy = True
    store.ok = False
    assert client.get("/ready").status_code == 503 and "database" in client.get("/ready").text
    store.ok = True
    publisher.degraded_reason = "kafka delivery failing: timed out"
    assert client.get("/ready").status_code == 503
    publisher.degraded_reason = None
    assert client.get("/ready").status_code == 200


def test_no_model_at_startup_is_not_ready_but_live() -> None:
    loader = FakeLoader()
    loader.fail = True
    cache = ModelCache("m", "champion", loader)
    service = PredictionService(cache, None, None, None)
    with TestClient(create_app(service, Settings())) as client:
        assert client.get("/health").status_code == 200
        assert (
            client.get("/ready").status_code == 503
            and "no model loaded" in client.get("/ready").text
        )
        assert client.post("/predict/raw", json={"features": _features()}).status_code == 503
        loader.fail = False
        client.post("/model/refresh", headers={"X-Admin-Token": "dev-admin-token"})
        assert client.get("/ready").status_code == 200


def test_persist_failure_is_reported_not_hidden(stack) -> None:  # noqa: ANN001
    client, _, _, _, store, publisher = stack
    store.ok = False
    r = client.post("/predict/raw", json={"features": _features()})
    assert r.status_code == 200 and r.json()["persisted"] is False
    assert len(publisher.events) == 1  # the event still goes out, flagged by the persisted field


def test_metrics_endpoint_exposes_prometheus(stack) -> None:  # noqa: ANN001
    client = stack[0]
    client.post("/predict/raw", json={"features": _features()})
    text = client.get("/metrics").text
    assert "amel_inference_predictions_total" in text and "amel_inference_model_loaded 1.0" in text
