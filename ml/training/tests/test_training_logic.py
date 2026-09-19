"""Pure logic: no MLflow server, no Postgres, no Feast."""

from __future__ import annotations

import numpy as np
import pandas as pd
from amel_common.schemas import HIGGS_FEATURE_NAMES
from amel_training.config import TrainingConfig
from amel_training.evaluate import compute_metrics, confusion, feature_importances
from amel_training.promote import PromotionCriteria, evaluate_promotion
from amel_training.provenance import dataset_version
from amel_training.split import split_frame


def _frame(n: int = 400, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        rng.normal(size=(n, len(HIGGS_FEATURE_NAMES))), columns=list(HIGGS_FEATURE_NAMES)
    )
    df["entity_id"] = [f"higgs-{i:09d}" for i in range(n)]
    df["event_timestamp"] = pd.Timestamp("2026-09-18 20:00:00") + pd.to_timedelta(np.arange(n), "s")
    df["target"] = (df["lepton_pt"] > 0).astype("int8")
    return df


def test_split_is_reproducible_and_stratified() -> None:
    df = _frame()
    a = split_frame(df, seed=42, validation_fraction=0.15, test_fraction=0.15)
    b = split_frame(df, seed=42, validation_fraction=0.15, test_fraction=0.15)
    assert a.X_test.index.tolist() == b.X_test.index.tolist()
    sizes = a.sizes()
    assert sizes["test"] == 60 and abs(sizes["validation"] - 60) <= 1
    assert sum(sizes.values()) == 400
    assert abs(a.y_test.mean() - df["target"].mean()) < 0.05
    c = split_frame(df, seed=7, validation_fraction=0.15, test_fraction=0.15)
    assert a.X_test.index.tolist() != c.X_test.index.tolist()


def test_split_partitions_are_disjoint_and_cover_everything() -> None:
    df = _frame()
    s = split_frame(df, seed=1, validation_fraction=0.2, test_fraction=0.1)
    idx = [set(s.X_train.index), set(s.X_val.index), set(s.X_test.index)]
    assert not (idx[0] & idx[1]) and not (idx[0] & idx[2]) and not (idx[1] & idx[2])
    assert idx[0] | idx[1] | idx[2] == set(df.index)


def test_dataset_version_fingerprint_depends_on_content_not_order() -> None:
    df = _frame()
    v1 = dataset_version(df, "higgs_features", "1")
    v2 = dataset_version(df.sample(frac=1, random_state=3), "higgs_features", "1")
    assert v1.fingerprint == v2.fingerprint
    assert v1.n_rows == 400 and v1.max_entity_id == "higgs-000000399"
    changed = df.copy()
    changed["target"] = changed["target"].where(changed.index != 0, 1 - changed["target"])
    assert dataset_version(changed, "higgs_features", "1").fingerprint != v1.fingerprint


def test_metrics_and_confusion_on_a_perfect_predictor() -> None:
    y = np.array([0, 1, 1, 0, 1])
    m = compute_metrics(y, y, y.astype(float))
    assert m["accuracy"] == 1.0 and m["roc_auc"] == 1.0 and m["f1"] == 1.0
    assert confusion(y, y) == {"tn": 2, "fp": 0, "fn": 0, "tp": 3}


def test_feature_importances_sorted_desc() -> None:
    fi = feature_importances(np.array([0.1, 0.7, 0.2]), ["a", "b", "c"])
    assert fi["feature"].tolist() == ["b", "c", "a"]


CRITERIA = PromotionCriteria(min_test_accuracy=0.66, min_test_roc_auc=0.70, max_regression=0.005)


def test_first_promotion_needs_only_the_floors() -> None:
    d = evaluate_promotion({"test_accuracy": 0.68, "test_roc_auc": 0.72}, None, CRITERIA)
    assert d.approved and any("first promotion" in r for r in d.reasons)


def test_below_floor_is_rejected_with_a_reason() -> None:
    d = evaluate_promotion({"test_accuracy": 0.60, "test_roc_auc": 0.72}, None, CRITERIA)
    assert not d.approved and any("< floor" in r for r in d.reasons)


def test_regression_beyond_tolerance_is_rejected_but_small_dips_pass() -> None:
    champ = {"test_accuracy": 0.700, "test_roc_auc": 0.75}
    worse = evaluate_promotion({"test_accuracy": 0.690, "test_roc_auc": 0.74}, champ, CRITERIA)
    assert not worse.approved and any("regresses champion" in r for r in worse.reasons)
    dip = evaluate_promotion({"test_accuracy": 0.697, "test_roc_auc": 0.74}, champ, CRITERIA)
    assert dip.approved


def test_missing_metrics_are_rejected() -> None:
    assert not evaluate_promotion({}, None, CRITERIA).approved


def test_config_tree_params_carry_the_seed(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("TRAINING_MAX_DEPTH", "5")
    monkeypatch.setenv("TRAINING_RANDOM_SEED", "9")
    cfg = TrainingConfig()
    assert cfg.tree_params() == {
        "criterion": "gini",
        "max_depth": 5,
        "min_samples_leaf": 200,
        "random_state": 9,
    }


def test_blank_env_values_mean_unset(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("TRAINING_AS_OF", "")
    monkeypatch.setenv("TRAINING_GIT_SHA", "  ")
    cfg = TrainingConfig()
    assert cfg.as_of is None and cfg.git_sha is None
    monkeypatch.setenv("TRAINING_AS_OF", "2026-09-19T21:00:00")
    assert TrainingConfig().as_of is not None
