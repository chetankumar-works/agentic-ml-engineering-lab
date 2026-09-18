from datetime import UTC, datetime

import pytest
from amel_common.schemas import HIGGS_FEATURE_NAMES, FeatureEvent, LabelEvent
from pydantic import ValidationError


def _features() -> dict[str, float]:
    return dict.fromkeys(HIGGS_FEATURE_NAMES, 0.0)


def test_feature_event_accepts_exact_higgs_feature_set() -> None:
    event = FeatureEvent(
        event_id="feat-1",
        entity_id="higgs-1",
        event_timestamp=datetime.now(UTC),
        features=_features(),
        trace_id="trace-1",
    )
    assert len(event.features) == 28


def test_feature_event_rejects_missing_feature() -> None:
    incomplete = _features()
    del incomplete["lepton_pt"]

    with pytest.raises(ValidationError):
        FeatureEvent(
            event_id="feat-1",
            entity_id="higgs-1",
            event_timestamp=datetime.now(UTC),
            features=incomplete,
            trace_id="trace-1",
        )


def test_feature_event_rejects_unexpected_feature() -> None:
    extra = _features()
    extra["not_a_real_feature"] = 1.0

    with pytest.raises(ValidationError):
        FeatureEvent(
            event_id="feat-1",
            entity_id="higgs-1",
            event_timestamp=datetime.now(UTC),
            features=extra,
            trace_id="trace-1",
        )


def test_label_event_rejects_target_out_of_range() -> None:
    with pytest.raises(ValidationError):
        LabelEvent(
            event_id="label-1",
            entity_id="higgs-1",
            target=2,
            label_timestamp=datetime.now(UTC),
        )
