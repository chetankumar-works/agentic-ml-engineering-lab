# Runs Feast CLI commands (apply / materialize) or the retrieval demo
# script against ml/feature_repo. Feast's dependencies turned out to
# resolve cleanly alongside the rest of the uv workspace (unlike
# Airflow — see DECISIONS.md ADR-0004), so this reuses the same
# per-package sync pattern as infra/db/Dockerfile rather than needing
# its own isolated environment.
#
# Build context is the repo root (see infra/docker-compose.yml).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY services ./services
COPY ml ./ml

RUN uv sync --frozen --no-dev --package amel-feature-repo

WORKDIR /app/ml/feature_repo
ENV PATH="/app/.venv/bin:$PATH"
# No fixed ENTRYPOINT/CMD — infra/docker-compose.yml's feast-apply,
# feast-materialize, and feast-demo services each pass their own command
# (`feast apply`, `feast materialize-incremental ...`,
# `python scripts/demo_retrieval.py`) against this same image.
