# Build context is the repo root (see infra/docker-compose.yml) so this
# can see the whole uv workspace and match uv.lock exactly.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY services ./services
COPY ml ./ml
COPY apps ./apps

RUN uv sync --frozen --no-dev --package stream-ingestor

ENV PATH="/app/.venv/bin:$PATH"
RUN useradd --create-home --uid 1000 amel
USER amel

EXPOSE 8002
CMD ["python", "-m", "stream_ingestor"]
