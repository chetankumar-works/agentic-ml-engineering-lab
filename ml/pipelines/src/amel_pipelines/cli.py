"""`amel-pipeline compile | run-steps | run-docker | submit`."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path


def _params(args: argparse.Namespace) -> dict[str, object]:
    return {
        "max_rows": args.max_rows,
        "as_of": args.as_of or "",
        "feature_repo_path": args.feature_repo_path,
        "tracking_uri": args.tracking_uri,
        "max_depth": args.max_depth,
        "git_sha": args.git_sha or "",
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="amel-pipeline", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p_compile = sub.add_parser("compile", help="compile the pipeline to KFP IR YAML")
    p_compile.add_argument(
        "--out", type=Path, default=Path("ml/pipelines/compiled/higgs_training_pipeline.yaml")
    )

    for name, help_ in (
        ("run-steps", "run the six steps in-process (no Kubeflow) — the reference path"),
        (
            "run-docker",
            "run the compiled components locally, one container per task (kfp.local DockerRunner)",
        ),
        ("submit", "submit to a KFP API server (--host) and wait"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--max-rows", type=int, default=100_000)
        p.add_argument("--as-of", default=None)
        p.add_argument("--feature-repo-path", default="/app/ml/feature_repo")
        p.add_argument("--tracking-uri", default="http://mlflow:5000")
        p.add_argument("--max-depth", type=int, default=8)
        p.add_argument("--git-sha", default=None)
        if name == "run-docker":
            p.add_argument(
                "--network",
                default="amel_default",
                help="Docker network for task containers (so postgres/mlflow resolve)",
            )
            p.add_argument(
                "--pipeline-root",
                type=Path,
                default=Path.home() / ".cache" / "amel-kfp-local",
                help="host directory for task artifacts (bind-mounted into every task container)",
            )
        if name == "submit":
            p.add_argument("--host", required=True, help="e.g. http://localhost:8888")
            p.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args(argv)

    if args.command == "compile":
        from kfp import compiler

        from amel_pipelines.pipeline import higgs_training_pipeline

        args.out.parent.mkdir(parents=True, exist_ok=True)
        compiler.Compiler().compile(higgs_training_pipeline, str(args.out))
        print(f"compiled -> {args.out}")
        return 0

    if args.command == "run-steps":
        from datetime import datetime

        from amel_training.config import TrainingConfig
        from amel_training.steps import run_all

        cfg = TrainingConfig(
            max_rows=args.max_rows,
            as_of=datetime.fromisoformat(args.as_of) if args.as_of else None,
            feature_repo_path=Path(args.feature_repo_path),
            mlflow_tracking_uri=args.tracking_uri,
            max_depth=args.max_depth,
            git_sha=args.git_sha,
        )
        with tempfile.TemporaryDirectory() as tmp:
            result = run_all(cfg, Path(tmp))
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.command == "run-docker":
        from kfp import local

        from amel_pipelines.pipeline import higgs_training_pipeline

        # The task containers run as uid 1000 (the image's non-root user);
        # the pipeline root is bind-mounted into them, so it must be
        # writable by that uid — create it here as the invoking user and
        # run the containers as that same uid.
        root = args.pipeline_root.resolve()
        root.mkdir(parents=True, exist_ok=True)
        local.init(
            runner=local.DockerRunner(
                network=args.network,
                user=f"{os.getuid()}:{os.getgid()}",
                # what Compose / the k8s Secret would inject; on Kubernetes the
                # components get these from `amel-secrets` (Milestone 8)
                environment={
                    "HOME": "/tmp",
                    "DATABASE_URL": os.environ.get(
                        "DATABASE_URL",
                        "postgresql+psycopg://amel:amel_dev_password@postgres:5432/amel",
                    ),
                    "MLFLOW_DISABLE_AGENT_HINT": "1",
                    "GIT_PYTHON_REFRESH": "quiet",
                },
            ),
            pipeline_root=str(root),
        )
        higgs_training_pipeline(**_params(args))
        print("local Docker run finished")
        return 0

    if args.command == "submit":
        from kfp import Client

        from amel_pipelines.pipeline import higgs_training_pipeline

        client = Client(host=args.host)
        run = client.create_run_from_pipeline_func(
            higgs_training_pipeline, arguments=_params(args), enable_caching=False
        )
        print(f"submitted run {run.run_id}")
        result = run.wait_for_run_completion(timeout=args.timeout)
        state = result.state
        print(f"run {run.run_id}: {state}")
        return 0 if str(state).upper().endswith("SUCCEEDED") else 1
    return 1


if __name__ == "__main__":
    sys.exit(main())
