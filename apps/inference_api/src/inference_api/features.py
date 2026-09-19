"""Online features through the Feast feature server over HTTP — the same
definitions training used offline (`higgs_features`), which is the
train/serve-skew guarantee. Kept HTTP (not the Feast SDK) so this image
carries no Feast dependency tree; see DECISIONS.md ADR-0005."""

from __future__ import annotations

import httpx
from amel_common.schemas import HIGGS_FEATURE_NAMES


class FeatureFetchError(RuntimeError):
    pass


class FeastOnlineClient:
    def __init__(self, base_url: str, feature_view: str, timeout_seconds: float) -> None:
        self._base = base_url.rstrip("/")
        self._refs = [f"{feature_view}:{name}" for name in HIGGS_FEATURE_NAMES]
        self._client = httpx.Client(base_url=self._base, timeout=timeout_seconds)

    def health(self) -> bool:
        try:
            return self._client.get("/health").status_code == 200
        except httpx.HTTPError:
            return False

    def get_features(self, entity_id: str) -> dict[str, float] | None:
        """All 28 features for one entity, or None if the online store has
        no row for it (never materialized, or not yet curated)."""
        try:
            r = self._client.post(
                "/get-online-features",
                json={"features": self._refs, "entities": {"entity_id": [entity_id]}},
            )
            r.raise_for_status()
        except httpx.HTTPError as exc:
            raise FeatureFetchError(f"feast-server: {exc}") from exc
        body = r.json()
        names = body["metadata"]["feature_names"]
        values = {name: col["values"][0] for name, col in zip(names, body["results"], strict=True)}
        features = {name: values.get(name) for name in HIGGS_FEATURE_NAMES}
        if any(v is None for v in features.values()):
            return None
        return {k: float(v) for k, v in features.items() if v is not None}

    def close(self) -> None:
        self._client.close()
