"""Deliberate malformed-event generation. These payloads are built as raw
dicts and never pass through a Pydantic model — that's the point: they
model what a buggy upstream producer, a schema mismatch, or wire
corruption actually looks like, so the stream ingestor's DLQ path has
something real to catch.
"""

from __future__ import annotations

import random
from typing import Any

_FEATURE_STRATEGIES = ("drop_field", "wrong_type", "extra_field", "truncated_features")
_LABEL_STRATEGIES = ("drop_field", "target_out_of_range", "wrong_type")


def corrupt_feature_payload(rng: random.Random, payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload)
    strategy = rng.choice(_FEATURE_STRATEGIES)
    if strategy == "drop_field":
        payload.pop(rng.choice(["entity_id", "event_timestamp", "features"]), None)
    elif strategy == "wrong_type":
        payload["features"] = "not-a-dict"
    elif strategy == "extra_field" and isinstance(payload.get("features"), dict):
        payload["features"] = {**payload["features"], "unexpected_field": 1.0}
    elif strategy == "truncated_features" and isinstance(payload.get("features"), dict):
        keys = list(payload["features"].keys())
        drop = keys[: max(1, len(keys) // 2)]
        payload["features"] = {k: v for k, v in payload["features"].items() if k not in drop}
    payload["_malformed_strategy"] = strategy
    return payload


def corrupt_label_payload(rng: random.Random, payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload)
    strategy = rng.choice(_LABEL_STRATEGIES)
    if strategy == "drop_field":
        payload.pop(rng.choice(["entity_id", "target", "label_timestamp"]), None)
    elif strategy == "target_out_of_range":
        payload["target"] = rng.choice([2, -1, 99])
    elif strategy == "wrong_type":
        payload["target"] = "signal"
    payload["_malformed_strategy"] = strategy
    return payload
