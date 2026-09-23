# Runs `amel-train train|promote|show` / `amel-pipeline ...` against the
# live stack, and is the base_image of every KFP component (Milestone 9). Same
# per-package uv sync pattern as infra/feast/Dockerfile — the training
# package, Feast and MLflow all resolved together in the workspace lock.
#
# Build context is the repo root (see infra/docker-compose.yml).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY services ./services
COPY ml ./ml
COPY apps ./apps

# amel-pipelines pulls in amel-training + kfp, so this one image serves
# `amel-train`, `amel-pipeline`, and the KFP executor inside each task pod.
RUN uv sync --frozen --no-dev --package amel-pipelines

ENV PATH="/app/.venv/bin:$PATH" \
    TRAINING_FEATURE_REPO_PATH=/app/ml/feature_repo \
    MLFLOW_DISABLE_AGENT_HINT=1 \
    GIT_PYTHON_REFRESH=quiet
RUN useradd --create-home --uid 1000 amel
USER amel
# No fixed ENTRYPOINT: as a KFP base_image the executor supplies its own
# command (`sh -c ... python -m kfp.dsl.executor_main`), which a
# hard-wired `amel-train` entrypoint would swallow. Callers pass the full
# command (`amel-train train`, `amel-pipeline run-steps`, ...).
CMD ["amel-train", "train"]
