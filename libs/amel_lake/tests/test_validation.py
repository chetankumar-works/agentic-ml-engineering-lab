from datetime import UTC, datetime

import pandas as pd
from amel_common.schemas import HIGGS_FEATURE_NAMES
from amel_lake.validation import features_schema, labels_schema, validate


def _valid_features_df(n: int) -> pd.DataFrame:
    now = datetime.now(UTC).replace(tzinfo=None)
    data = {
        "event_id": [f"feat-{i}" for i in range(n)],
        "entity_id": [f"higgs-{i}" for i in range(n)],
        "event_timestamp": [now for _ in range(n)],
    }
    for name in HIGGS_FEATURE_NAMES:
        data[name] = [0.5] * n
    return pd.DataFrame(data)


def _valid_labels_df(n: int) -> pd.DataFrame:
    now = datetime.now(UTC).replace(tzinfo=None)
    return pd.DataFrame(
        {
            "event_id": [f"label-{i}" for i in range(n)],
            "entity_id": [f"higgs-{i}" for i in range(n)],
            "target": [i % 2 for i in range(n)],
            "label_timestamp": [now for _ in range(n)],
        }
    )


def test_all_valid_features_pass_through_unchanged() -> None:
    df = _valid_features_df(5)
    valid_df, report = validate(df, features_schema())
    assert report.total_rows == 5
    assert report.valid_rows == 5
    assert report.invalid_rows == 0
    assert report.invalid_fraction == 0.0
    assert len(valid_df) == 5


def test_out_of_range_feature_is_quarantined_and_reported() -> None:
    df = _valid_features_df(4)
    df.loc[0, "lepton_pt"] = 999.0  # outside the generous bound

    valid_df, report = validate(df, features_schema())

    assert report.total_rows == 4
    assert report.invalid_rows == 1
    assert report.valid_rows == 3
    assert len(valid_df) == 3
    assert "feat-0" not in valid_df["event_id"].to_numpy()
    assert any(f["column"] == "lepton_pt" for f in report.failures)


def test_duplicate_event_id_quarantines_both_copies() -> None:
    df = _valid_features_df(3)
    df.loc[1, "event_id"] = "feat-0"  # duplicate of row 0

    valid_df, report = validate(df, features_schema())

    assert report.invalid_rows == 2
    assert report.valid_rows == 1


def test_labels_reject_out_of_range_target() -> None:
    df = _valid_labels_df(3)
    df.loc[0, "target"] = 7

    valid_df, report = validate(df, labels_schema())

    assert report.invalid_rows == 1
    assert report.valid_rows == 2


def test_empty_dataframe_is_trivially_valid() -> None:
    df = _valid_features_df(0)
    valid_df, report = validate(df, features_schema())
    assert report.total_rows == 0
    assert report.invalid_fraction == 0.0
