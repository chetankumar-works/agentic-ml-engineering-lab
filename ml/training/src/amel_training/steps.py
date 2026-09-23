"""The training workflow as six file-to-file steps (Milestone 9).

`train.py` runs the workflow in one process; this module is the same
logic cut at the boundaries a pipeline needs — every step reads and
writes files, so it can run as a Kubeflow component, in a local runner,
or in a plain loop (`run_all`). The functions it calls are the ones
`train.py` already uses, so the two paths produce the same fingerprint
and metrics for the same config (proven in the Milestone 9 report).

Artifacts between steps: Parquet for data, skops for the model (safer
than pickle; the one trusted type is the tree's node storage), JSON for
everything else.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from amel_training.config import TrainingConfig
from amel_training.dataset import FEATURE_COLUMNS, TARGET_COLUMN

SKOPS_TRUSTED = ["sklearn.tree._tree.Tree"]


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str))


def load_training_dataset(
    dataset_path: Path,
    version_path: Path,
    *,
    max_rows: int,
    as_of: str | None,
    feature_repo_path: str,
) -> dict[str, Any]:
    """Feast offline (point-in-time) retrieval → Parquet + dataset version."""
    from feast import FeatureStore

    from amel_training.dataset import feature_view_version, load_training_frame
    from amel_training.provenance import dataset_version

    cfg = TrainingConfig(
        max_rows=max_rows,
        as_of=datetime.fromisoformat(as_of) if as_of else None,
        feature_repo_path=Path(feature_repo_path),
    )
    store = FeatureStore(repo_path=str(cfg.feature_repo_path))
    fv_version = feature_view_version(store, cfg.feature_view)
    df = load_training_frame(cfg, store)
    version = dataset_version(df, cfg.feature_view, fv_version)
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dataset_path, index=False)
    info = {**version.as_dict(), "as_of": (cfg.as_of or datetime.now(UTC)).isoformat()}
    _write_json(version_path, info)
    return info


def validate_training_dataset(
    dataset_path: Path,
    report_path: Path,
    *,
    min_rows: int = 1_000,
    max_class_imbalance: float = 0.9,
) -> dict[str, Any]:
    """Fail loudly before spending compute: enough rows, the 28 columns,
    no NaNs, a binary label that isn't (almost) constant."""
    df = pd.read_parquet(dataset_path)
    problems: list[str] = []
    if len(df) < min_rows:
        problems.append(f"only {len(df)} rows (< {min_rows})")
    missing = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing:
        problems.append(f"missing feature columns: {missing}")
    else:
        nan_cols = [c for c in FEATURE_COLUMNS if df[c].isna().any()]
        if nan_cols:
            problems.append(f"NaNs in {nan_cols}")
    if TARGET_COLUMN not in df.columns:
        problems.append("missing target")
    else:
        labels = set(df[TARGET_COLUMN].unique().tolist())
        if not labels <= {0, 1}:
            problems.append(f"target values {sorted(labels)} not in {{0, 1}}")
        elif len(df):
            share = float(df[TARGET_COLUMN].mean())
            if max(share, 1 - share) > max_class_imbalance:
                problems.append(
                    f"class imbalance {max(share, 1 - share):.3f} > {max_class_imbalance}"
                )
    report = {
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
        "positive_fraction": float(df[TARGET_COLUMN].mean()) if TARGET_COLUMN in df else None,
        "problems": problems,
        "valid": not problems,
    }
    _write_json(report_path, report)
    if problems:
        raise ValueError("training dataset failed validation: " + "; ".join(problems))
    return report


def split_dataset(
    dataset_path: Path,
    out_dir: Path,
    *,
    seed: int,
    validation_fraction: float,
    test_fraction: float,
) -> dict[str, int]:
    from amel_training.split import split_frame

    df = pd.read_parquet(dataset_path)
    splits = split_frame(
        df, seed=seed, validation_fraction=validation_fraction, test_fraction=test_fraction
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, X, y in (
        ("train", splits.X_train, splits.y_train),
        ("validation", splits.X_val, splits.y_val),
        ("test", splits.X_test, splits.y_test),
    ):
        X.assign(**{TARGET_COLUMN: y.to_numpy()}).to_parquet(
            out_dir / f"{name}.parquet", index=False
        )
    sizes = splits.sizes()
    _write_json(out_dir / "split_sizes.json", sizes)
    return sizes


def train_model(
    train_path: Path, model_path: Path, *, tree_params: dict[str, Any]
) -> dict[str, Any]:
    import skops.io as sio
    from sklearn.tree import DecisionTreeClassifier

    df = pd.read_parquet(train_path)
    started = time.monotonic()
    model = DecisionTreeClassifier(**tree_params).fit(df[FEATURE_COLUMNS], df[TARGET_COLUMN])
    fit_seconds = time.monotonic() - started
    model_path.parent.mkdir(parents=True, exist_ok=True)
    sio.dump(model, model_path)
    info = {
        "fit_seconds": fit_seconds,
        "tree_depth": int(model.get_depth()),
        "n_leaves": int(model.get_n_leaves()),
        "params": tree_params,
    }
    _write_json(model_path.with_suffix(".json"), info)
    return info


def evaluate_model(model_path: Path, splits_dir: Path, out_dir: Path) -> dict[str, Any]:
    import numpy as np
    import skops.io as sio

    from amel_training.evaluate import (
        compute_metrics,
        confusion,
        feature_importances,
        plot_confusion,
        plot_importances,
    )

    model = sio.load(model_path, trusted=SKOPS_TRUSTED)
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics: dict[str, float] = {}
    for name in ("validation", "test"):
        df = pd.read_parquet(splits_dir / f"{name}.parquet")
        X, y = df[FEATURE_COLUMNS], np.asarray(df[TARGET_COLUMN])
        pred = model.predict(X)
        proba = model.predict_proba(X)[:, 1]
        metrics.update({f"{name}_{k}": v for k, v in compute_metrics(y, pred, proba).items()})
        cm = confusion(y, pred)
        _write_json(out_dir / f"confusion_matrix_{name}.json", cm)
        plot_confusion(cm, out_dir / f"confusion_matrix_{name}.png", f"{name} split")
    importances = feature_importances(model.feature_importances_, FEATURE_COLUMNS)
    importances.to_csv(out_dir / "feature_importances.csv", index=False)
    plot_importances(importances, out_dir / "feature_importances.png")
    _write_json(out_dir / "metrics.json", metrics)
    return metrics


def register_model(
    model_path: Path,
    metrics_dir: Path,
    version_path: Path,
    splits_dir: Path,
    *,
    tracking_uri: str,
    experiment_name: str,
    registered_model_name: str,
    candidate_alias: str,
    git_sha: str | None,
    tree_params: dict[str, Any],
    pipeline_run_id: str | None = None,
) -> dict[str, Any]:
    """One MLflow run carrying everything the earlier steps produced,
    then a registered version aliased `candidate` — the same contract as
    `train.py`, so Milestone 4's `promote` and Milestone 5's serving work
    unchanged on pipeline-trained models."""
    import mlflow
    import skops.io as sio
    from mlflow import MlflowClient
    from mlflow.models import infer_signature

    from amel_training.train import _model_version_for_run

    model = sio.load(model_path, trusted=SKOPS_TRUSTED)
    metrics = json.loads((metrics_dir / "metrics.json").read_text())
    version = json.loads(version_path.read_text())
    sizes = json.loads((splits_dir / "split_sizes.json").read_text())
    model_info = json.loads(model_path.with_suffix(".json").read_text())
    sample = pd.read_parquet(splits_dir / "train.parquet").head(5)[FEATURE_COLUMNS]

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=f"kfp-{pipeline_run_id}" if pipeline_run_id else None) as run:
        mlflow.log_params(tree_params)
        mlflow.log_params(
            {
                "as_of": version["as_of"],
                "dataset_fingerprint": version["fingerprint"],
                "dataset_n_rows": version["n_rows"],
                "feature_view": version["feature_view"],
                "feature_view_version": version["feature_view_version"],
                "n_features": len(FEATURE_COLUMNS),
                "orchestrator": "kfp" if pipeline_run_id else "steps",
            }
        )
        mlflow.set_tags(
            {
                "git_sha": git_sha or "unknown",
                "feature_view": version["feature_view"],
                "feature_view_version": version["feature_view_version"],
                "dataset_fingerprint": version["fingerprint"],
                "kfp_run_id": pipeline_run_id or "",
                "trained_at": datetime.now(UTC).isoformat(),
            }
        )
        mlflow.log_dict(version, "dataset_version.json")
        mlflow.log_dict({"features": FEATURE_COLUMNS}, "feature_definitions.json")
        mlflow.log_dict(sizes, "split_sizes.json")
        mlflow.log_dict(model_info, "tree_shape.json")
        mlflow.log_metrics({**metrics, "fit_seconds": model_info["fit_seconds"]})
        mlflow.log_artifacts(str(metrics_dir))
        logged = mlflow.sklearn.log_model(
            model,
            name="model",
            signature=infer_signature(sample, model.predict(sample)),
            input_example=sample.head(3),
            registered_model_name=registered_model_name,
            skops_trusted_types=SKOPS_TRUSTED,
        )
        client = MlflowClient()
        mv = _model_version_for_run(client, registered_model_name, run.info.run_id)
        client.set_registered_model_alias(registered_model_name, candidate_alias, mv.version)
        for key, value in (
            ("git_sha", git_sha or "unknown"),
            ("dataset_fingerprint", version["fingerprint"]),
            ("feature_view", f"{version['feature_view']}:v{version['feature_view_version']}"),
            ("test_accuracy", f"{metrics['test_accuracy']:.4f}"),
            ("test_roc_auc", f"{metrics['test_roc_auc']:.4f}"),
            ("orchestrator", "kfp" if pipeline_run_id else "steps"),
        ):
            client.set_model_version_tag(registered_model_name, mv.version, key, value)
    return {"run_id": run.info.run_id, "model_version": mv.version, "model_uri": logged.model_uri}


def run_all(cfg: TrainingConfig, workdir: Path) -> dict[str, Any]:
    """The six steps in-process — the non-Kubeflow path, used to prove the
    decomposition matches `train.py` and as the fastest dev loop."""
    from amel_training.provenance import git_sha

    d = workdir
    version = load_training_dataset(
        d / "dataset.parquet",
        d / "dataset_version.json",
        max_rows=cfg.max_rows,
        as_of=cfg.as_of.isoformat() if cfg.as_of else None,
        feature_repo_path=str(cfg.feature_repo_path),
    )
    validate_training_dataset(d / "dataset.parquet", d / "validation.json")
    split_dataset(
        d / "dataset.parquet",
        d / "splits",
        seed=cfg.random_seed,
        validation_fraction=cfg.validation_fraction,
        test_fraction=cfg.test_fraction,
    )
    train_model(d / "splits" / "train.parquet", d / "model.skops", tree_params=cfg.tree_params())
    metrics = evaluate_model(d / "model.skops", d / "splits", d / "evaluation")
    reg = register_model(
        d / "model.skops",
        d / "evaluation",
        d / "dataset_version.json",
        d / "splits",
        tracking_uri=cfg.mlflow_tracking_uri,
        experiment_name=cfg.experiment_name,
        registered_model_name=cfg.registered_model_name,
        candidate_alias=cfg.candidate_alias,
        git_sha=git_sha(cfg.git_sha),
        tree_params=cfg.tree_params(),
    )
    return {"dataset_fingerprint": version["fingerprint"], "metrics": metrics, **reg}
