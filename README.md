# Agentic ML Engineering Lab (AMEL)

AMEL is a fully executable, learning-oriented ML engineering platform. It
is not a toy demo — every component listed below is meant to actually run,
end to end, on the UCI HIGGS dataset, using a real streaming ingestion
pipeline, a real feature store, real orchestration, real observability,
and a small but genuine agentic control plane on top.

The goal is not "generate a lot of code." It is to build production ML
engineering understanding — streaming systems, idempotency, feature
stores, orchestration, observability, Kubernetes, durable workflows, and
agentic automation with real safety controls — one working vertical slice
at a time.

See `AMEL_KICKOFF_PROMPT.md` for the full original specification this
project is built against, `ARCHITECTURE.md` for the system design,
`PROJECT_STATE.md` for exactly what's built and what's next, and
`DECISIONS.md` for the reasoning behind non-obvious choices.

## Status

Milestone 1 (PostgreSQL + Kafka + source simulator + stream ingestor) —
see `PROJECT_STATE.md` for current detail and exact acceptance numbers.

## Quickstart

Requires [`uv`](https://docs.astral.sh/uv/) (installs without root) and
Docker with Compose v2:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

From the repo root:

```bash
make install      # uv sync --all-packages — creates .venv pinned to Python 3.12 (see DECISIONS.md ADR-0001)
make lint          # ruff check
make fmt           # ruff format
make typecheck     # mypy
make test          # pytest — unit tests only, no infra required

make up            # docker compose up: Postgres, Kafka, migrations, source_simulator, stream_ingestor
make logs          # follow all service logs
make smoke         # Milestone 1 acceptance check against a running `make up` stack
make down          # stop everything
```

`.python-version` pins this project to Python 3.12 regardless of the
system's default `python3`. `uv` reads it automatically.

The source simulator downloads the ~2.8GB UCI HIGGS dataset zip on first
start (see `services/source_simulator/src/source_simulator/dataset.py`)
and caches it under `./data/raw/` (bind-mounted, gitignored) — this takes
a while on a slow connection but only happens once.

## Repository layout

Not all of these directories exist yet — see `DECISIONS.md` ADR-0002 for
the planned layout and `PROJECT_STATE.md` for what's actually been built.

```
apps/          user/operator-facing services (inference_api, platform_api, webhook_service)
services/      internal platform services (source_simulator, stream_ingestor)
ml/            Feast feature repo, training package
sdk/python/    AMELClient SDK
cli/           Typer CLI
infra/         docker compose, Kubernetes manifests, migrations
tests/         cross-cutting/integration/e2e tests
```

## Documentation

| File | Purpose |
|---|---|
| `ARCHITECTURE.md` | System design and how components fit together |
| `PROJECT_STATE.md` | Current milestone, what works, what's next — read this first when resuming |
| `LEARNING_LOG.md` | What was built and why, per subsystem, with concepts and interview questions |
| `DECISIONS.md` | Architecture Decision Records |
| `SECURITY.md` | Secret handling, auth model, reporting |
| `RUNBOOKS.md` | Operational runbooks for failure scenarios |
