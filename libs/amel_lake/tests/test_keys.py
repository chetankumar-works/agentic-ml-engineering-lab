from datetime import date

from amel_lake.keys import bronze_key, gold_training_key, silver_key, validation_report_key


def test_bronze_key_is_deterministic_and_partitioned_by_date() -> None:
    d = date(2026, 9, 20)
    key = bronze_key("features", d, run_id="scheduled__2026-09-20T00:00:00+00:00")
    assert key == "higgs/features/dt=2026-09-20/run_id=scheduled__2026-09-20T00_00_00_00_00.parquet"


def test_bronze_key_sanitizes_colons_and_plus_in_run_id() -> None:
    key = bronze_key("labels", date(2026, 1, 1), run_id="manual__2026-01-01T00:00:00+00:00")
    assert ":" not in key
    assert "+" not in key


def test_bronze_and_silver_keys_share_shape() -> None:
    d = date(2026, 1, 1)
    assert bronze_key("features", d, "r1") == silver_key("features", d, "r1")


def test_gold_training_key_has_no_date_partition() -> None:
    key = gold_training_key("r1")
    assert key == "training/run_id=r1.parquet"


def test_validation_report_key_scoped_under_artifacts() -> None:
    key = validation_report_key("r1")
    assert key == "artifacts/validation_reports/run_id=r1.json"
