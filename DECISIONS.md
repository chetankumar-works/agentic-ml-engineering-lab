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

---

## ADR-0003: Kafka in KRaft mode, no ZooKeeper

**Problem.** `AMEL_KICKOFF_PROMPT.md` asks for "modern Kafka
configuration." Kafka historically required a separate ZooKeeper
ensemble for cluster metadata/controller election; KRaft (Kafka's own
Raft-based metadata quorum, GA since Kafka 3.3+) removes that dependency.

**Decision.** Run Kafka in KRaft mode, combined `broker,controller`
roles, single node, using the official `apache/kafka:3.9.0` image — no
ZooKeeper container.

**Reason.** ZooKeeper-based Kafka is legacy as of 2026: Kafka 4.x removed
ZooKeeper support entirely, and even on 3.x it's explicitly the
deprecated path. Running it here would teach a pattern that's actively
being retired industry-wide, which cuts against this project's stated
learning goal. KRaft also means one fewer stateful service to run,
migrate, and reason about in Compose/Kubernetes later, with no loss of
any concept this project needs to demonstrate (consumer groups,
partitions, offsets, replication factor are all identical from a
client's perspective).

**Tradeoffs.** Single-node KRaft (one combined broker+controller) is a
dev/learning topology, not a production one — a real deployment would
run an odd number (3+) of controller nodes for quorum safety. This
project's local Compose/kind/minikube targets never need that, so it's
not implemented, but it's worth knowing this is the gap between "runs
locally" and "runs in production" for this specific piece.

**Future reconsideration trigger.** If Milestone 8+ (Kubernetes) needs to
demonstrate multi-broker behavior (e.g. partition reassignment,
replication-factor-2+ failover) that a single-node topology can't show,
add a multi-node KRaft cluster at that point — the topic/consumer code
in `services/stream_ingestor` and `services/source_simulator` doesn't
change either way.

---

## ADR-0004: Airflow gets its own image, own Postgres database, and `airflow standalone`

**Problem.** Milestone 2 needed Airflow orchestrating the bronze/silver/
gold batch pipeline. Three separate decisions: (1) how does Airflow's own
Python environment relate to the uv workspace the rest of the project
uses; (2) where does Airflow's metadata database live; (3) which of
Airflow's several deployment topologies (standalone, or separate
webserver/scheduler/triggerer/dag-processor containers, with
LocalExecutor/CeleryExecutor/KubernetesExecutor) fits a single-developer
local learning setup.

**Decision.**
1. Airflow is installed in its own Docker image (`infra/airflow/
   Dockerfile`, based on `apache/airflow:3.3.2-python3.12`), *not* added
   to the uv workspace. `libs/amel_common`/`libs/amel_db`/`libs/amel_lake`
   are `pip install`ed into that image directly from their source
   directories (each is still a normal, self-contained package with its
   own `pyproject.toml`).
2. Airflow's metadata database is a second database (`airflow`) on the
   *same* Postgres container AMEL already runs — not a separate Postgres
   instance, not SQLite.
3. Airflow runs as a single container via `airflow standalone`
   (`AIRFLOW__CORE__EXECUTOR=LocalExecutor`), bundling the API
   server/webserver, scheduler, triggerer, and DAG processor in one
   process group.

**Reason.**
1. This is exactly the trigger ADR-0002 anticipated: Airflow ships an
   extremely strict, version-pinned dependency set (via its own
   "constraints files"), and forcing it into the same `uv.lock` as
   `confluent-kafka`/`fastapi`/etc. would be fighting the tool rather
   than using it. Empirically, `apache/airflow:3.3.2-python3.12` already
   bundles compatible-or-newer versions of nearly everything AMEL's own
   packages need (pydantic 2.13, SQLAlchemy 2.0, psycopg 3.3, boto3,
   pandas, pyarrow, structlog) via its `amazon`/`postgres` providers —
   only `alembic` and `pandera` needed adding. Isolating Airflow's
   environment cost almost nothing here and avoids a much worse problem
   later (a real lockfile conflict blocking `uv sync` for the whole repo).
2. A second Postgres *container* would be pure infrastructure overhead
   (another volume, another healthcheck, another thing that can fail)
   for zero benefit at this scale — a second *database* on the existing
   instance gets the real isolation that matters (Airflow's ~50 internal
   tables never mixing with AMEL's own schemas) without it.
3. `airflow standalone` is explicitly Airflow's own recommended path for
   local development and trying things out — it self-initializes the
   metadata DB and a default admin user on first boot, which matches
   this project's "one `make up` and everything works" goal better than
   hand-wiring four separate long-running containers plus a Redis broker
   for CeleryExecutor would.

**Tradeoffs.** `airflow standalone` prints a warning that SimpleAuthManager
(the default auth backend here) stores passwords in plaintext and isn't
meant for production — correct, and fine: this never runs anywhere but a
developer's own machine. LocalExecutor also means all task parallelism is
bounded by one container's CPU/memory, and — more substantively — every
DAG task in `infra/airflow/dags/higgs_pipeline.py` hands data to the next
task via local disk (`/opt/airflow/staging/<run_id>/`, a Docker volume)
rather than object storage, which only works because LocalExecutor keeps
every task on the same machine. A distributed executor (Celery/
Kubernetes) would need that hand-off rewritten to go through MinIO or
similar between every task, not just for the bronze/silver landing itself.

**Future reconsideration trigger.** If Milestone 10 (KEDA/autoscaling) or
a later milestone needs to demonstrate genuinely distributed task
execution, revisit both the executor (LocalExecutor → CeleryExecutor or
KubernetesExecutor) and the local-staging hand-off pattern in the DAG
tasks together — they're coupled, per the tradeoff above.
