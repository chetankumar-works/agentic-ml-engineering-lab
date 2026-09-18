# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 0 — Repository skeleton, tooling, GitHub remote.** Complete.

## Completed work

- Verified local environment (see table below) and resolved the
  Python 3.14-vs-dependencies compatibility question: project is pinned
  to Python 3.12 via `uv`, not the system's 3.14. Full reasoning in
  `DECISIONS.md` ADR-0001.
- `pyproject.toml`: ruff (lint + format) and mypy configured; pytest
  configured with `testpaths = ["tests"]`; `[tool.uv] package = false`
  (virtual project, no installable package yet).
- `Makefile`: `install`/`lint`/`fmt`/`typecheck`/`test` are real and
  passing; `up`/`down`/`logs`/`migrate`/`seed`/`smoke` are intentional
  `echo`-and-exit-0 stubs until Milestone 1.
- `.gitignore` covers secrets, Python/uv artifacts, future Docker volume
  dirs, Airflow/MLflow local state, and the HIGGS dataset (never
  committed — Milestone 1 documents how to fetch it).
- `.env.example` — minimal for now (`ENVIRONMENT`, `LOG_LEVEL`); real
  service variables get appended as each service is built, not
  pre-declared speculatively.
- `tests/test_environment.py` — asserts the pinned interpreter is 3.12
  and that `ssl`/`sqlite3`/`bz2`/`lzma` import correctly.
- Required docs created and non-placeholder: `README.md`,
  `ARCHITECTURE.md`, `DECISIONS.md`, `SECURITY.md`, `RUNBOOKS.md`,
  `LEARNING_LOG.md`, this file.
- GitHub remote created at
  `github.com/chetankumar-works/agentic-ml-engineering-lab` (public),
  initial commit pushed to `main`, tagged `milestone-0`.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute", re-verified 2026-09-18)

| Tool | Version | Note |
|---|---|---|
| Node.js | v22.23.2 | via nvm, not yet used (frontend is Milestone 15) |
| System Python | 3.14.4 | **not** what this project uses — see below |
| Project Python | 3.12.14 | via `uv python install 3.12`, standalone build, pinned in `.python-version` |
| uv | 0.12.16 | installed to `~/.local/bin`, no root required |
| Docker | 29.7.2 | Docker Desktop, WSL2 integration |
| Docker Compose | v5.4.0 | |
| git | 2.53.0 | |
| gh | 2.46.0 | authenticated as `chetankumar-works`, verified via `gh auth status` |

**Python version decision**: see `DECISIONS.md` ADR-0001. Short version:
this WSL2 host has no passwordless `sudo` and lacks the build toolchain
`pyenv` needs, so Python 3.12 is provisioned via `uv python install`
(prebuilt standalone interpreter, no compilation, no root) rather than
`pyenv` or `deadsnakes`.

## Current known failures / gaps

- No runtime services exist yet — this is expected at Milestone 0, not a
  regression. `make up`/`down`/`logs`/`migrate`/`seed`/`smoke` are
  placeholder stubs by design until Milestone 1.
- No CI workflow yet (GitHub Actions lands at Milestone 7, though a
  minimal lint/test workflow could reasonably be pulled forward if it
  becomes annoying to keep `main` green by hand — not done yet, no
  need identified).

## Commands that work today

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh   # if uv isn't installed
make install     # uv sync — creates .venv on pinned Python 3.12
make lint         # ruff check .          -> passes
make fmt          # ruff format .
make typecheck    # mypy .                -> passes
make test         # pytest                -> 2 passed
```

## Next task

**Milestone 1 — PostgreSQL, Kafka, source simulator, stream consumer.**

Acceptance criteria (from `AMEL_KICKOFF_PROMPT.md`): 100,000+ events can
be streamed and persisted; duplicates do not create duplicate records;
malformed events reach a DLQ; consumer restart is safe.

Concretely, in order:
1. Docker Compose services for PostgreSQL and Kafka (KRaft mode, no
   Zookeeper — "modern Kafka configuration" per spec); document topic
   creation for `higgs.features.v1`, `higgs.labels.v1`,
   `higgs.features.dlq`, `higgs.labels.dlq`, `predictions.v1`,
   `platform.events.v1`, `audit.events.v1`.
2. Alembic setup + initial migration for the `landing` schema tables
   (`landing.higgs_feature_events`, `landing.higgs_label_events`) with
   `event_id` uniqueness constraints.
3. Versioned Pydantic schemas for feature/label events.
4. `services/source_simulator` — HIGGS dataset fetch/streaming, event
   publishing with configurable rate/burst/duplicate/malformed/pause-
   resume behavior, health + metrics endpoints.
5. `services/stream_ingestor` — consumer group, controlled offset
   commits, idempotent Postgres sink (`INSERT ... ON CONFLICT`), DLQ
   handling, retry/backoff, graceful shutdown, structured logs.
6. Document the at-least-once-delivery + idempotent-sink argument
   explicitly (the 6-step crash/redelivery scenario in the spec) in
   `LEARNING_LOG.md`'s Milestone 1 entry.
7. Failure exercises: duplicate delivery, malformed event, consumer
   restart mid-batch — each gets a `RUNBOOKS.md` entry.
8. Full Definition of Done pass, commit `feat(milestone-1): ...`, push,
   tag `milestone-1`.
