# Runs `amel-train train|promote|show` against the live stack. Same
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

RUN uv sync --frozen --no-dev --package amel-training

ENV PATH="/app/.venv/bin:$PATH" \
    TRAINING_FEATURE_REPO_PATH=/app/ml/feature_repo \
    MLFLOW_DISABLE_AGENT_HINT=1 \
    GIT_PYTHON_REFRESH=quiet
ENTRYPOINT ["amel-train"]
CMD ["train"]
