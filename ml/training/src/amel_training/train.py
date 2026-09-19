"""One training run: load (Feast offline) → split → fit → evaluate →
log everything to MLflow → register the model → alias it `candidate`.

Nothing here promotes to `champion`; that is a separate, explicit,
audited step (`promote.py`) — a run producing a good model is not the
same decision as serving it."""

from __future__ import annotations

import platform
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import mlflow
import mlflow.data.pandas_dataset
import numpy as np
import sklearn
from feast import FeatureStore
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.tree import DecisionTreeClassifier

from amel_training.config import TrainingConfig
from amel_training.dataset import FEATURE_COLUMNS, feature_view_version, load_training_frame
from amel_training.evaluate import (
    compute_metrics,
    confusion,
    feature_importances,
    plot_confusion,
    plot_importances,
)
from amel_training.provenance import dataset_version, git_sha
from amel_training.split import split_frame


@dataclass(frozen=True)
class TrainingResult:
    run_id: str
    model_version: str
    metrics: dict[str, float]
    dataset_fingerprint: str
    duration_seconds: float


def run_training(cfg: TrainingConfig) -> TrainingResult:
    started = time.monotonic()
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.experiment_name)

    store = FeatureStore(repo_path=str(cfg.feature_repo_path))
    fv_version = feature_view_version(store, cfg.feature_view)
    df = load_training_frame(cfg, store)
    version = dataset_version(df, cfg.feature_view, fv_version)
    splits = split_frame(
        df,
        seed=cfg.random_seed,
        validation_fraction=cfg.validation_fraction,
        test_fraction=cfg.test_fraction,
    )
    sha = git_sha(cfg.git_sha)

    with mlflow.start_run() as run:
        # --- reproducibility inputs, logged before anything is fit ---
        mlflow.log_params(cfg.tree_params())
        mlflow.log_params(
            {
                "random_seed": cfg.random_seed,
                "validation_fraction": cfg.validation_fraction,
                "test_fraction": cfg.test_fraction,
                "max_rows": cfg.max_rows,
                "as_of": (cfg.as_of or datetime.now(UTC)).isoformat(),
                "feature_view": cfg.feature_view,
                "feature_view_version": fv_version,
                "n_features": len(FEATURE_COLUMNS),
                "dataset_fingerprint": version.fingerprint,
                "dataset_n_rows": version.n_rows,
                "sklearn_version": sklearn.__version__,
                "python_version": platform.python_version(),
            }
        )
        mlflow.set_tags(
            {
                "git_sha": sha or "unknown",
                "feature_view": cfg.feature_view,
                "feature_view_version": fv_version,
                "dataset_fingerprint": version.fingerprint,
                "trained_at": datetime.now(UTC).isoformat(),
            }
        )
        mlflow.log_dict(version.as_dict(), "dataset_version.json")
        mlflow.log_dict({"features": FEATURE_COLUMNS}, "feature_definitions.json")
        mlflow.log_dict(splits.sizes(), "split_sizes.json")
        mlflow.log_input(
            mlflow.data.pandas_dataset.from_pandas(
                df.head(1000), name=f"higgs_training_{version.fingerprint}"
            ),
            context="training",
        )

        # --- fit ---
        fit_started = time.monotonic()
        model = DecisionTreeClassifier(**cfg.tree_params())
        model.fit(splits.X_train, splits.y_train)
        fit_seconds = time.monotonic() - fit_started

        # --- evaluate on validation and the untouched test split ---
        all_metrics: dict[str, float] = {}
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            for name, X, y in (
                ("validation", splits.X_val, splits.y_val),
                ("test", splits.X_test, splits.y_test),
            ):
                pred = model.predict(X)
                proba = model.predict_proba(X)[:, 1]
                metrics = compute_metrics(np.asarray(y), pred, proba)
                cm = confusion(np.asarray(y), pred)
                all_metrics.update({f"{name}_{k}": v for k, v in metrics.items()})
                mlflow.log_dict(cm, f"confusion_matrix_{name}.json")
                mlflow.log_artifact(
                    str(
                        plot_confusion(cm, tmpdir / f"confusion_matrix_{name}.png", f"{name} split")
                    )
                )
            importances = feature_importances(model.feature_importances_, FEATURE_COLUMNS)
            importances.to_csv(tmpdir / "feature_importances.csv", index=False)
            mlflow.log_artifact(str(tmpdir / "feature_importances.csv"))
            mlflow.log_artifact(
                str(plot_importances(importances, tmpdir / "feature_importances.png"))
            )
            mlflow.log_dict(
                {"tree_depth": int(model.get_depth()), "n_leaves": int(model.get_n_leaves())},
                "tree_shape.json",
            )
        all_metrics["fit_seconds"] = fit_seconds
        mlflow.log_metrics(all_metrics)

        # --- model + signature, registered, aliased candidate ---
        signature = infer_signature(splits.X_train.head(5), model.predict(splits.X_train.head(5)))
        logged = mlflow.sklearn.log_model(
            model,
            name="model",
            signature=signature,
            input_example=splits.X_train.head(3),
            registered_model_name=cfg.registered_model_name,
            # MLflow 3 serializes sklearn models with skops (not pickle) and
            # refuses types it cannot audit; a decision tree's node storage
            # is one. Declaring it here is recorded in the MLmodel flavor
            # config, so loaders (Milestone 5) need no extra trust list.
            skops_trusted_types=["sklearn.tree._tree.Tree"],
        )
        client = MlflowClient()
        mv = _model_version_for_run(client, cfg.registered_model_name, run.info.run_id)
        client.set_registered_model_alias(
            cfg.registered_model_name, cfg.candidate_alias, mv.version
        )
        for key, value in (
            ("git_sha", sha or "unknown"),
            ("dataset_fingerprint", version.fingerprint),
            ("feature_view", f"{cfg.feature_view}:v{fv_version}"),
            ("test_accuracy", f"{all_metrics['test_accuracy']:.4f}"),
            ("test_roc_auc", f"{all_metrics['test_roc_auc']:.4f}"),
        ):
            client.set_model_version_tag(cfg.registered_model_name, mv.version, key, value)

        duration = time.monotonic() - started
        mlflow.log_metric("training_duration_seconds", duration)
        mlflow.log_dict(
            {"model_uri": logged.model_uri, "registered_version": mv.version}, "registration.json"
        )

    return TrainingResult(
        run_id=run.info.run_id,
        model_version=mv.version,
        metrics=all_metrics,
        dataset_fingerprint=version.fingerprint,
        duration_seconds=duration,
    )


def _model_version_for_run(client: MlflowClient, name: str, run_id: str):  # noqa: ANN202
    versions = client.search_model_versions(f"name = '{name}' and run_id = '{run_id}'")
    if not versions:
        raise RuntimeError(f"model registration for run {run_id} did not produce a version")
    return max(versions, key=lambda v: int(v.version))
