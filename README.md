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

Milestone 5 (FastAPI inference serving the cached MLflow champion over
raw or Feast-online features, persisting and publishing every
prediction) on top of Milestone 4's reproducible training + MLflow
registry, Milestone 3's Feast/Redis feature store, Milestone
2's Airflow + MinIO batch pipeline and Milestone 1's PostgreSQL + Kafka
streaming ingestion — see `PROJECT_STATE.md` for current detail and
`MILESTONE_REPORT.md` for measured acceptance evidence per milestone.

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

make up            # docker compose up: Postgres, Kafka, MinIO, Redis, migrations, Airflow,
                   #   feast-apply, feast-server, source_simulator, stream_ingestor
make logs          # follow all service logs
make smoke         # Milestone 1 acceptance check against a running `make up` stack
make feast-materialize   # Milestone 3: chunked backfill of curated features into Redis
make feast-demo          # Milestone 3: historical (point-in-time) + online retrieval demo
make failure-simulator-wedge   # pauses Kafka ~30s; simulator probes must go 503 and recover
make train                     # Milestone 4: one training run -> MLflow, registered, aliased candidate
make promote DECIDED_BY=you    # explicit, audited candidate -> champion (exit 2 if criteria fail)
make model-show                # current candidate/champion
make down          # stop everything
```

MLflow UI: http://localhost:5000. Airflow UI: http://localhost:8080.
Inference API: http://localhost:8003/docs (OpenAPI).

Memory: this stack has been measured at roughly 5–6 GB resident with
everything up (Kafka ~0.9 GB, Airflow ~1.5 GB, Redis ~0.5 GB at ~1M
entities, feast-server ~0.2–0.4 GB, MLflow ~0.5 GB, inference-api
~0.25 GB). Every Feast container is
memory-capped; do not run unbounded `feast materialize` — see
`RUNBOOKS.md`.

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
| `ARCHITECTURE.md` | System design, start-to-finish diagram, and a glossary — **start here if you're new** |
| `PROJECT_STATE.md` | Current milestone, what works, what's next — read this first when resuming |
| `LEARNING_LOG.md` | What was built and why, per subsystem, with concepts and interview questions |
| `MISTAKES.md` | Every real bug hit during this build, root cause and fix, in under a minute per entry |
| `DECISIONS.md` | Architecture Decision Records |
| `SECURITY.md` | Secret handling, auth model, reporting |
| `RUNBOOKS.md` | Operational runbooks for failure scenarios |
| `MILESTONE_REPORT.md` | Per-milestone report: what was built, how acceptance was proven (measured), decisions, bugs, gaps |
