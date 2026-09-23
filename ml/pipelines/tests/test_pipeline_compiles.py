"""The pipeline compiles to a KFP IR with six executors on the training image."""

from __future__ import annotations

from pathlib import Path

import yaml
from kfp import compiler


def test_pipeline_compiles_with_six_containerized_components(tmp_path: Path) -> None:
    from amel_pipelines.pipeline import PIPELINE_NAME, TRAINING_IMAGE, higgs_training_pipeline

    out = tmp_path / "p.yaml"
    compiler.Compiler().compile(higgs_training_pipeline, str(out))
    # With platform config (kfp-kubernetes) the IR is two YAML documents:
    # the PipelineSpec and a PlatformSpec.
    docs = list(yaml.safe_load_all(out.read_text()))
    spec = docs[0]
    platform = docs[1]["platforms"]["kubernetes"]["deploymentSpec"]["executors"]
    assert any("secretAsEnv" in e for e in platform.values()), (
        "DATABASE_URL must come from a Secret"
    )
    executors = spec["deploymentSpec"]["executors"]
    assert len(executors) == 6
    assert {e["container"]["image"] for e in executors.values()} == {TRAINING_IMAGE}
    assert spec["pipelineInfo"]["name"] == PIPELINE_NAME
    tasks = spec["root"]["dag"]["tasks"]
    assert set(tasks) == {
        "load-training-dataset",
        "validate-training-dataset",
        "split-dataset",
        "train-model",
        "evaluate-model",
        "register-model",
    }
    # the DAG edges: register depends on evaluate, evaluate on train, train on split ...
    assert "evaluate-model" in tasks["register-model"]["dependentTasks"]
    assert "validate-training-dataset" in tasks["split-dataset"]["dependentTasks"]
