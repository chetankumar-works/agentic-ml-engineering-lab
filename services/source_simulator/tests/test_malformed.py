import random

from source_simulator.malformed import corrupt_feature_payload, corrupt_label_payload


def _valid_feature_payload() -> dict:
    return {
        "event_id": "feat-1",
        "entity_id": "higgs-000000001",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "schema_version": "1.0.0",
        "source": "source_simulator",
        "features": {"lepton_pt": 1.0, "lepton_eta": 2.0},
        "trace_id": "abc123",
    }


def _valid_label_payload() -> dict:
    return {
        "event_id": "label-1",
        "entity_id": "higgs-000000001",
        "target": 1,
        "label_timestamp": "2026-01-01T00:00:00Z",
        "schema_version": "1.0.0",
    }


def test_corrupt_feature_payload_always_changes_something() -> None:
    rng = random.Random(0)
    original = _valid_feature_payload()

    corrupted = corrupt_feature_payload(rng, original)

    assert corrupted != {**original}
    assert "_malformed_strategy" in corrupted
    # original dict passed in must not be mutated in place
    assert "_malformed_strategy" not in original


def test_corrupt_label_target_out_of_range_is_reachable() -> None:
    rng = random.Random(2)  # deterministic: picks target_out_of_range first
    corrupted = corrupt_label_payload(rng, _valid_label_payload())

    if corrupted["_malformed_strategy"] == "target_out_of_range":
        assert corrupted["target"] not in (0, 1)
