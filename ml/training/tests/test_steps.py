"""The six file-to-file steps on synthetic data (no Feast/MLflow/Postgres):
validate → split → train → evaluate, plus the failure branches."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from amel_common.schemas import HIGGS_FEATURE_NAMES
from amel_training import steps


def _frame(n: int = 3000, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(rng.normal(size=(n, 28)).astype("float32"), columns=list(HIGGS_FEATURE_NAMES))
    df["entity_id"] = [f"higgs-{i:09d}" for i in range(n)]
    df["event_timestamp"] = pd.Timestamp("2026-09-18") + pd.to_timedelta(np.arange(n), "s")
    df["target"] = (df["lepton_pt"] + 0.3 * df["m_bb"] > 0).astype("int8")
    return df


def test_validate_split_train_evaluate_chain(tmp_path: Path) -> None:
    ds = tmp_path / "dataset.parquet"
    _frame().to_parquet(ds, index=False)
    report = steps.validate_training_dataset(ds, tmp_path / "validation.json", min_rows=1000)
    assert report["valid"] and report["rows"] == 3000
    sizes = steps.split_dataset(
        ds, tmp_path / "splits", seed=1, validation_fraction=0.2, test_fraction=0.2
    )
    assert sum(sizes.values()) == 3000 and (tmp_path / "splits" / "test.parquet").exists()
    info = steps.train_model(
        tmp_path / "splits" / "train.parquet",
        tmp_path / "model.skops",
        tree_params={
            "criterion": "gini",
            "max_depth": 4,
            "min_samples_leaf": 20,
            "random_state": 1,
        },
    )
    assert info["tree_depth"] <= 4 and (tmp_path / "model.skops").exists()
    metrics = steps.evaluate_model(tmp_path / "model.skops", tmp_path / "splits", tmp_path / "eval")
    assert 0.7 < metrics["test_accuracy"] <= 1.0 and "validation_roc_auc" in metrics
    assert json.loads((tmp_path / "eval" / "metrics.json").read_text()) == metrics
    assert (tmp_path / "eval" / "confusion_matrix_test.png").exists()
    assert (tmp_path / "eval" / "feature_importances.csv").exists()


def test_validation_rejects_bad_datasets(tmp_path: Path) -> None:
    small = _frame(50)
    small.to_parquet(tmp_path / "small.parquet", index=False)
    with pytest.raises(ValueError, match="only 50 rows"):
        steps.validate_training_dataset(
            tmp_path / "small.parquet", tmp_path / "r.json", min_rows=100
        )
    nan = _frame(2000)
    nan.loc[0, "m_bb"] = np.nan
    nan.to_parquet(tmp_path / "nan.parquet", index=False)
    with pytest.raises(ValueError, match="NaNs"):
        steps.validate_training_dataset(tmp_path / "nan.parquet", tmp_path / "r.json", min_rows=100)
    constant = _frame(2000)
    constant["target"] = 1
    constant.to_parquet(tmp_path / "constant.parquet", index=False)
    with pytest.raises(ValueError, match="class imbalance"):
        steps.validate_training_dataset(
            tmp_path / "constant.parquet", tmp_path / "r.json", min_rows=100
        )
    report = json.loads((tmp_path / "r.json").read_text())
    assert report["valid"] is False and report["problems"]


def test_split_is_deterministic_across_processes_via_files(tmp_path: Path) -> None:
    ds = tmp_path / "dataset.parquet"
    _frame().to_parquet(ds, index=False)
    steps.split_dataset(ds, tmp_path / "a", seed=7, validation_fraction=0.15, test_fraction=0.15)
    steps.split_dataset(ds, tmp_path / "b", seed=7, validation_fraction=0.15, test_fraction=0.15)
    a = pd.read_parquet(tmp_path / "a" / "test.parquet")
    b = pd.read_parquet(tmp_path / "b" / "test.parquet")
    pd.testing.assert_frame_equal(a, b)
