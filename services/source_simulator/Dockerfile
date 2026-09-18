# Build context is the repo root (see infra/docker-compose.yml) so this
# can see the whole uv workspace and match uv.lock exactly.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY services ./services

RUN uv sync --frozen --no-dev --package source-simulator

ENV PATH="/app/.venv/bin:$PATH"
RUN useradd --create-home --uid 1000 amel && mkdir -p /data && chown amel:amel /data
USER amel

EXPOSE 8001
CMD ["python", "-m", "source_simulator"]
