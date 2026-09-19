"""The champion model, loaded once and swapped atomically.

Why a cache: loading a model means an MLflow registry call plus an
artifact download plus deserialization — hundreds of milliseconds and a
dependency on MLflow being up. A request must never pay that, and MLflow
being down after start-up must not take inference down. Why *controlled*
refresh: a new champion should reach serving through a deliberate
action (an operator call, or a bounded poll) that can be observed and
rolled back — not through whatever happened to be at the alias when a
request arrived."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import mlflow
import mlflow.sklearn
from amel_common.logging import get_logger
from mlflow import MlflowClient

logger = get_logger(component="model_cache")


@dataclass(frozen=True)
class ModelMetadata:
    name: str
    alias: str
    version: str
    run_id: str | None
    git_sha: str | None
    dataset_fingerprint: str | None
    feature_view: str | None
    test_accuracy: str | None
    loaded_at: datetime

    def as_dict(self) -> dict[str, Any]:
        d = self.__dict__.copy()
        d["loaded_at"] = self.loaded_at.isoformat()
        return d


@dataclass(frozen=True)
class LoadedModel:
    model: Any  # sklearn estimator with predict / predict_proba
    metadata: ModelMetadata


Loader = Callable[[str, str], LoadedModel]


def mlflow_loader(tracking_uri: str) -> Loader:
    """Resolve `models:/<name>@<alias>` to a version, load it, and carry
    the version's tags (written by amel-train) as serving metadata."""

    def load(name: str, alias: str) -> LoadedModel:
        mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient()
        mv = client.get_model_version_by_alias(name, alias)
        model = mlflow.sklearn.load_model(f"models:/{name}/{mv.version}")
        tags = dict(mv.tags)
        return LoadedModel(
            model=model,
            metadata=ModelMetadata(
                name=name,
                alias=alias,
                version=mv.version,
                run_id=mv.run_id,
                git_sha=tags.get("git_sha"),
                dataset_fingerprint=tags.get("dataset_fingerprint"),
                feature_view=tags.get("feature_view"),
                test_accuracy=tags.get("test_accuracy"),
                loaded_at=datetime.now(UTC),
            ),
        )

    return load


@dataclass
class ModelCache:
    name: str
    alias: str
    loader: Loader
    _current: LoadedModel | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)
    last_error: str | None = None
    last_error_at: float | None = None
    refresh_count: int = 0

    @property
    def current(self) -> LoadedModel | None:
        return self._current  # attribute read is atomic in CPython

    @property
    def loaded(self) -> bool:
        return self._current is not None

    def load_initial(self) -> None:
        """Start-up load. Failure here means the service is *not ready*
        (no model) but still *live* (a later refresh may succeed)."""
        self.refresh()

    def refresh(self) -> tuple[str | None, str | None]:
        """Load whatever the alias points at now and swap it in under the
        lock. Returns (previous_version, current_version). A failed load
        leaves the previous model serving and records the error."""
        with self._lock:
            previous = self._current.metadata.version if self._current else None
            try:
                loaded = self.loader(self.name, self.alias)
            except Exception as exc:  # noqa: BLE001 — recorded, surfaced via /health and /model
                self.last_error = repr(exc)
                self.last_error_at = time.monotonic()
                logger.error("model_refresh_failed", error=repr(exc), serving=previous)
                return previous, previous
            if self._current is not None and loaded.metadata.version == previous:
                self.last_error = None  # the registry answered; a stale error is misleading
                logger.info("model_refresh_noop", version=previous)
                return previous, previous
            self._current = loaded
            self.last_error = None
            self.refresh_count += 1
            logger.info("model_swapped", previous=previous, current=loaded.metadata.version)
            return previous, loaded.metadata.version
