"""`higgs_training_pipeline`: six containerized KFP v2 components, each a
thin wrapper around one function in `amel_training.steps`.

How KFP turns this into Kubernetes workloads (DECISIONS.md ADR-0011):
`compile()` serializes the DAG to an IR YAML (PipelineSpec). Submitting
it to a KFP API server produces an Argo Workflow; each task becomes a
Pod running `base_image` with KFP's executor as the entrypoint; the
executor materializes the function's inputs (artifact URIs → local
paths under /tmp, parameters → JSON), runs the function, and uploads
the outputs to the pipeline root (object storage) so the next task's
Pod can download them. Parameters/metrics are recorded in ML Metadata.
`kfp.local.DockerRunner` does the same on a laptop with one container
per task and a local pipeline root — no cluster needed.

Every component pins `base_image` to the AMEL training image (which
already contains kfp, hence `install_kfp_package=False`), so the code
that runs in the pod is exactly the code that runs in `amel-train train`.
"""

# No `from __future__ import annotations` here on purpose: KFP inspects the
# real annotation objects (Input[Dataset] etc.) to build the component spec;
# string annotations break compilation.
import os

from kfp import dsl, kubernetes
from kfp.dsl import Dataset, Input, Metrics, Model, Output

# Non-`latest` tag on purpose: Kubernetes pulls `:latest` with policy Always,
# which fails for an image that only exists in the kind node (loaded with
# `kind load docker-image`). Override for a registry image in CI/prod.
TRAINING_IMAGE = os.environ.get("AMEL_TRAINING_IMAGE", "amel-training:local")
PIPELINE_NAME = "higgs-training"


@dsl.component(base_image=TRAINING_IMAGE, install_kfp_package=False)
def load_training_dataset(
    dataset: Output[Dataset],
    dataset_version: Output[Dataset],
    max_rows: int,
    as_of: str,
    feature_repo_path: str,
) -> str:
    from pathlib import Path

    from amel_training.steps import load_training_dataset as step

    info = step(
        Path(dataset.path),
        Path(dataset_version.path),
        max_rows=max_rows,
        as_of=as_of or None,
        feature_repo_path=feature_repo_path,
    )
    dataset.metadata.update({"rows": info["n_rows"], "fingerprint": info["fingerprint"]})
    return str(info["fingerprint"])


@dsl.component(base_image=TRAINING_IMAGE, install_kfp_package=False)
def validate_training_dataset(
    dataset: Input[Dataset], report: Output[Dataset], min_rows: int
) -> bool:
    from pathlib import Path

    from amel_training.steps import validate_training_dataset as step

    result = step(Path(dataset.path), Path(report.path), min_rows=min_rows)
    return bool(result["valid"])


@dsl.component(base_image=TRAINING_IMAGE, install_kfp_package=False)
def split_dataset(
    dataset: Input[Dataset],
    splits: Output[Dataset],
    seed: int,
    validation_fraction: float,
    test_fraction: float,
) -> int:
    from pathlib import Path

    from amel_training.steps import split_dataset as step

    sizes = step(
        Path(dataset.path),
        Path(splits.path),
        seed=seed,
        validation_fraction=validation_fraction,
        test_fraction=test_fraction,
    )
    splits.metadata.update(sizes)
    return int(sizes["train"])


@dsl.component(base_image=TRAINING_IMAGE, install_kfp_package=False)
def train_model(
    splits: Input[Dataset],
    model: Output[Model],
    criterion: str,
    max_depth: int,
    min_samples_leaf: int,
    seed: int,
) -> float:
    from pathlib import Path

    from amel_training.steps import train_model as step

    info = step(
        Path(splits.path) / "train.parquet",
        Path(model.path) / "model.skops",
        tree_params={
            "criterion": criterion,
            "max_depth": max_depth,
            "min_samples_leaf": min_samples_leaf,
            "random_state": seed,
        },
    )
    model.metadata.update({"tree_depth": info["tree_depth"], "n_leaves": info["n_leaves"]})
    return float(info["fit_seconds"])


@dsl.component(base_image=TRAINING_IMAGE, install_kfp_package=False)
def evaluate_model(
    model: Input[Model], splits: Input[Dataset], evaluation: Output[Metrics]
) -> float:
    from pathlib import Path

    from amel_training.steps import evaluate_model as step

    metrics = step(Path(model.path) / "model.skops", Path(splits.path), Path(evaluation.path))
    for k, v in metrics.items():
        evaluation.log_metric(k, float(v))
    return float(metrics["test_accuracy"])


@dsl.component(base_image=TRAINING_IMAGE, install_kfp_package=False)
def register_model(
    model: Input[Model],
    evaluation: Input[Metrics],
    dataset_version: Input[Dataset],
    splits: Input[Dataset],
    tracking_uri: str,
    experiment_name: str,
    registered_model_name: str,
    candidate_alias: str,
    git_sha: str,
    criterion: str,
    max_depth: int,
    min_samples_leaf: int,
    seed: int,
    pipeline_run_id: str,
) -> str:
    from pathlib import Path

    from amel_training.steps import register_model as step

    result = step(
        Path(model.path) / "model.skops",
        Path(evaluation.path),
        Path(dataset_version.path),
        Path(splits.path),
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        registered_model_name=registered_model_name,
        candidate_alias=candidate_alias,
        git_sha=git_sha or None,
        tree_params={
            "criterion": criterion,
            "max_depth": max_depth,
            "min_samples_leaf": min_samples_leaf,
            "random_state": seed,
        },
        pipeline_run_id=pipeline_run_id or None,
    )
    return str(result["model_version"])


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="AMEL HIGGS DecisionTree training as containerized KFP components",
)
def higgs_training_pipeline(
    max_rows: int = 100_000,
    as_of: str = "",
    feature_repo_path: str = "/app/ml/feature_repo",
    min_rows: int = 1_000,
    seed: int = 42,
    validation_fraction: float = 0.15,
    test_fraction: float = 0.15,
    criterion: str = "gini",
    max_depth: int = 8,
    min_samples_leaf: int = 200,
    tracking_uri: str = "http://mlflow:5000",
    experiment_name: str = "higgs_decision_tree",
    registered_model_name: str = "higgs_decision_tree",
    candidate_alias: str = "candidate",
    git_sha: str = "",
) -> None:
    loaded = load_training_dataset(
        max_rows=max_rows, as_of=as_of, feature_repo_path=feature_repo_path
    )
    validated = validate_training_dataset(dataset=loaded.outputs["dataset"], min_rows=min_rows)
    split = split_dataset(
        dataset=loaded.outputs["dataset"],
        seed=seed,
        validation_fraction=validation_fraction,
        test_fraction=test_fraction,
    ).after(validated)
    trained = train_model(
        splits=split.outputs["splits"],
        criterion=criterion,
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        seed=seed,
    )
    evaluated = evaluate_model(model=trained.outputs["model"], splits=split.outputs["splits"])
    register_model(
        model=trained.outputs["model"],
        evaluation=evaluated.outputs["evaluation"],
        dataset_version=loaded.outputs["dataset_version"],
        splits=split.outputs["splits"],
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        registered_model_name=registered_model_name,
        candidate_alias=candidate_alias,
        git_sha=git_sha,
        criterion=criterion,
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        seed=seed,
        # substituted by the backend (and by kfp.local) at run time; it lands
        # in the MLflow run as `kfp_run_id` so a model traces back to its run
        pipeline_run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,
    )
    # Resource hints — real limits on Kubernetes, ignored by the local runners.
    for task in (loaded, validated, split, trained, evaluated):
        task.set_memory_limit("2G").set_cpu_limit("1")
    # Configuration separation on Kubernetes (Milestone 8's rule): the Feast
    # offline query needs DATABASE_URL, which comes from the `amel-secrets`
    # Secret in the namespace KFP runs in — never from the pipeline
    # definition. kfp.local ignores platform-specific config, so the
    # Docker runner injects the same variable itself (cli.py).
    kubernetes.use_secret_as_env(
        loaded, secret_name="amel-secrets", secret_key_to_env={"DATABASE_URL": "DATABASE_URL"}
    )
