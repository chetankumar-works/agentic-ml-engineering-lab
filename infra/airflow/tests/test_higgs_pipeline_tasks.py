from datetime import UTC, datetime

import higgs_pipeline_tasks as tasks
import pandas as pd
from amel_common.schemas import HIGGS_FEATURE_NAMES


def _now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def test_chunked_splits_evenly() -> None:
    rows = [{"i": i} for i in range(5)]
    assert tasks._chunked(rows, 2) == [[{"i": 0}, {"i": 1}], [{"i": 2}, {"i": 3}], [{"i": 4}]]


def test_chunked_handles_empty_input() -> None:
    assert tasks._chunked([], 2) == []


def test_chunked_single_batch_when_size_exceeds_length() -> None:
    rows = [{"i": i} for i in range(3)]
    assert tasks._chunked(rows, 100) == [rows]


def test_watermark_bounds_is_a_plain_dataclass() -> None:
    now = _now_naive()
    bounds = tasks.WatermarkBounds(features_watermark=now, labels_watermark=now, upper_bound=now)
    assert bounds.upper_bound == now


def test_determine_high_watermark_defaults_to_epoch_when_unset(monkeypatch) -> None:
    from amel_lake.watermark import EPOCH

    monkeypatch.setattr(tasks, "get_watermark", lambda _name: EPOCH)
    upper = _now_naive()

    bounds = tasks.determine_high_watermark(upper)

    assert bounds.features_watermark == EPOCH
    assert bounds.labels_watermark == EPOCH
    assert bounds.upper_bound == upper


def test_transform_features_flattens_and_types_feature_columns() -> None:
    now = _now_naive()
    df = pd.DataFrame(
        [
            {
                "event_id": "feat-2",
                "entity_id": "higgs-2",
                "event_timestamp": now,
                "schema_version": "1.0.0",
                **dict.fromkeys(HIGGS_FEATURE_NAMES, "1.5"),  # deliberately str, must be coerced
            },
            {
                "event_id": "feat-1",
                "entity_id": "higgs-1",
                "event_timestamp": now,
                "schema_version": "1.0.0",
                **dict.fromkeys(HIGGS_FEATURE_NAMES, "0.5"),
            },
        ]
    )

    out = tasks.transform_features(df)

    assert out["lepton_pt"].dtype == "float64"
    assert list(out["event_id"]) == ["feat-1", "feat-2"]  # sorted


def test_transform_labels_types_target_as_int() -> None:
    now = _now_naive()
    df = pd.DataFrame(
        [
            {
                "event_id": "label-1",
                "entity_id": "higgs-1",
                "target": "1",
                "label_timestamp": now,
                "schema_version": "1.0.0",
            },
        ]
    )

    out = tasks.transform_labels(df)

    assert out["target"].dtype == "int64"
    assert out["target"].iloc[0] == 1
