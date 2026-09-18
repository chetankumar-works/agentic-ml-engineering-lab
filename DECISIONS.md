# Architecture Decision Records

Each entry: problem, options considered, decision, reason, tradeoffs,
future reconsideration trigger. Numbered sequentially, never renumbered or
deleted — superseded decisions get a new entry that links back.

---

## ADR-0001: Pin the project to Python 3.12 instead of the system Python 3.14

**Problem.** The system-wide `python3` on this WSL2 host is 3.14.4, a very
new release. Several core dependencies this project needs (Apache Airflow,
Feast, the Kubeflow Pipelines SDK, Pandera, MLflow) build native extensions
and gate their published wheels to specific CPython minor versions. New
CPython releases routinely take 6–12 months for the ML/data tooling
ecosystem to catch up with binary wheels.

**Options considered.**
1. Use system Python 3.14 and hope dependencies install; fall back to
   building from source or patching around missing wheels as they break.
2. Install Python 3.12 via `pyenv`, which compiles CPython from source
   against system build libraries (`libssl-dev`, `libsqlite3-dev`, etc.).
3. Install Python 3.12 via `uv python install`, which downloads a
   prebuilt, self-contained CPython (python-build-standalone) with no
   compilation and no system dev-package dependency.

**Decision.** Option 3: manage Python versions with `uv`, pinned to 3.12
via `.python-version`, with `uv` also used as the dependency
manager/lockfile tool (`uv sync`, `uv run`).

**Reason.** This environment has no passwordless `sudo` (interactive auth
required), and the standard build-essential/`libssl-dev`/`libsqlite3-dev`
toolchain pyenv needs to compile CPython from source is not installed —
`dpkg -l` showed only 1 of 5 required dev packages present. `uv python
install 3.12` downloaded a working, prebuilt 3.12.14 interpreter directly
under `~/.local/share/uv/python/` with no root access and no compilation
step (verified `ssl`, `sqlite3`, `bz2`, `lzma` all import correctly — see
`tests/test_environment.py`). `uv` also replaces the need for a separate
`pip`/`venv`/`pip-tools` workflow: one tool pins the interpreter, resolves
and locks dependencies (`uv.lock`), and runs commands inside the
associated `.venv`.

**Tradeoffs.** The project now depends on `uv` being installed
(`curl -LsSf https://astral.sh/uv/install.sh | sh`, installs to
`~/.local/bin`, no root needed) rather than assuming a bare
`python3`/`pip` toolchain. This is recorded here explicitly so a future
session or a fresh clone knows why `.python-version` says `3.12` when
`python3 --version` on the host reports 3.14, and knows to run `uv sync`
rather than `pip install`.

**Future reconsideration trigger.** Revisit once Airflow, Feast, the KFP
SDK, Pandera and MLflow all publish `cp314` wheels (check at the start of
whichever milestone first needs one of them) — or immediately if any of
those libraries turns out to need a system dependency `uv`'s standalone
build doesn't provide.

---

## ADR-0002: Monorepo layout, not one repo per service

**Problem.** AMEL spans many independently deployable units — ingestion
services, the inference API, the platform API, the webhook service, the
Python SDK, a CLI, a minimal web app, a Kubeflow pipeline, Kubernetes
manifests. These could each live in their own repository or together in
one.

**Decision.** Single repository (`agentic-ml-engineering-lab`) with a
top-level layout by concern, planned as (built incrementally, not created
as empty stubs today):

```
apps/              # user/operator-facing services: inference_api, platform_api, webhook_service
services/           # internal platform services: source_simulator, stream_ingestor
ml/                 # feature_repo (Feast), training package
sdk/python/         # AMELClient SDK
cli/                # Typer CLI
infra/              # docker compose, k8s manifests, dbt/alembic migrations
docs/               # supplementary docs beyond the required top-level *.md files
tests/              # cross-cutting/integration/e2e tests (each package also owns its own unit tests)
```

**Reason.** This project is explicitly for one developer to build deep,
connected understanding of how these subsystems interact — a monorepo
keeps cross-service changes (e.g. a schema change touching the simulator,
the consumer, and the training package) in one commit and one CI run,
which matches how the milestones are structured (each milestone is a
vertical slice, not an isolated service). A multi-repo split would also
require duplicating CI/CD, docs, and versioning conventions many times
over for no benefit at this scale.

**Tradeoffs.** A single `pyproject.toml`/lockfile at the root does not
scale forever if individual services need genuinely conflicting
dependency versions; some services will likely need their own
`pyproject.toml` once they have non-trivial, service-specific
dependencies (Airflow's dependency pins, in particular, are notoriously
strict and may conflict with Feast/MLflow in a single shared
environment). This is anticipated, not solved, here.

**Future reconsideration trigger.** If a service's dependency set
genuinely conflicts with another's in the shared environment (this is
already flagged as likely for Airflow), split that service into its own
`pyproject.toml` with its own `uv` environment rather than forcing one
shared lockfile — document that split here when it happens, and note
whether the CI/CD workflow needed to change to build/test it
independently.
