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

## ADR-0005: Feast on a Postgres offline store + SQL registry, Redis online, an always-on feature server, and chunked materialization

**Problem.** Milestone 3 needed a feature store with (1) an offline
source for point-in-time-correct training retrieval, (2) an online store
for low-latency inference lookups, (3) a registry, (4) a way for the
scheduled Airflow DAG to materialize newly curated rows into the online
store every five minutes, and (5) all of it inside a 15 GB WSL2 machine
that had already been crashed twice by memory exhaustion.

**Decision.**
1. **Offline store: Feast's Postgres offline store over
   `curated.higgs_features_flat`**, a VIEW (Alembic `0003`) that flattens
   `curated.higgs_features.features` (JSONB, one blob per event) into 28
   typed `float8` columns. Not the silver/gold Parquet in MinIO.
2. **Registry: Feast's SQL registry in its own `feast` database** on the
   existing Postgres container (same pattern as Airflow's metadata DB,
   ADR-0004). Not the default local `registry.db` file.
3. **Online store: Redis 7** (`redis-data` volume, host port 6380).
4. **Materialization runs in a long-running `feast-server` container**
   (`feast serve`, memory-capped at 1 GB), and the Airflow task
   `update_feature_store` calls its `POST /materialize` over HTTP. Feast
   is *not* installed in the Airflow image.
5. **All materialization is chunked** — the one-shot backfill
   (`ml/feature_repo/scripts/materialize.py`) and the per-run Airflow
   task both split their window into ≤25,000-row sub-windows (boundaries
   computed in SQL from actual row density) and call
   `store.materialize(start, end)` / `POST /materialize` once per
   sub-window. Never `feast materialize-incremental` over an unbounded
   window.

**Reason.**
1. Postgres vs Parquet offline store. `curated.higgs_features` is
   *already* the deduplicated, accumulated source of truth that
   Milestone 2 built (idempotent upserts, watermark-gated), and it has an
   index on `event_timestamp` — Feast's point-in-time join becomes an
   indexed SQL query on data that is never stale relative to the
   pipeline. The Parquet alternative (`silver/` in MinIO) is keyed by
   `run_id`, so one entity's row can exist in several objects (re-runs,
   wide re-extraction windows) and Feast's file offline store would have
   to scan every object to find the latest — correct only by accident of
   dedup, and slow. The cost of the Postgres choice is that Feast wants
   one column per feature, hence the VIEW: zero extra storage, generated
   from `HIGGS_FEATURE_NAMES` so it cannot drift from the feature list.
   The Parquet route becomes the right answer when the offline history
   outgrows Postgres (billions of rows, columnar scans for training) —
   the swap is one `source=` line in `definitions.py`, and this ADR is
   the trigger to revisit.
2. A file registry is a single local file that every Feast process
   (apply, server, demo, Airflow-side materialize) would need to share
   via a volume, and it has no concurrency story. The SQL registry gives
   every container the same registry over the network, plus
   `feature_view_version_history` for free — which is how "feature
   versioning" is actually tracked, alongside the explicit
   `tags={"version": ...}` on the view.
3. Redis is what the kickoff spec asks for and what Feast's online path
   is designed around: one key per entity, latest value only, ~0.5 KB
   per entity here (495 MB for 937k entities).
4. Always-on server vs the alternatives, and why this was a *deliberate*
   choice on a memory-starved machine: the DAG runs every 5 minutes, so
   whatever materializes must be reachable on that cadence. "On demand"
   would mean either Airflow spawning containers (needs the Docker
   socket inside Airflow — a real security/complexity step) or importing
   Feast in the Airflow image (Feast's dependency tree is exactly the
   kind ADR-0004 kept out of that image). One idle uvicorn process costs
   165 MB (measured; 361 MB after serving materialize calls), and the
   `mem_limit: 1g` means a runaway is OOM-killed *inside its container*
   rather than taking WSL2 down. Calling a feature-store *service* from
   the orchestrator is also how this boundary looks in production.
5. Feast's Postgres offline store materializes a window by loading every
   row into memory and converting each to protobufs — measured at
   ~14 KB/row, i.e. **12.1 GB RSS over the 885k-row table**, which the
   kernel OOM-killed (and which is almost certainly what crashed WSL2
   during the first Milestone 3 attempt, see RUNBOOKS.md). Chunking by
   row count bounds peak memory by chunk size, not table size: the same
   backfill ran at a measured peak of 875 MiB in 36 windows / 167 s.
   Feast records each window in the registry, so a re-run resumes.

**Tradeoffs.** Every container that touches Feast needs Postgres *and*
Redis reachable (no offline-only mode). The VIEW recomputes JSON
extraction on every read — fine at this scale with the timestamp index
doing the filtering; a materialized view or a real flat table is the
next step if offline reads become slow. `feast-server` is one more
always-on process (~165–360 MB). Per-run materialization is
synchronous inside the Airflow task (bounded by the 10-minute task
timeout; a 5-minute window at 100 events/s is ~30k rows, two
sub-windows, well under a minute). The Feast Postgres offline store
defaults to `sslmode=require`, which the local Postgres does not support
— `sslmode: disable` is set explicitly in `feature_store.yaml`.

**Future reconsideration trigger.** Move the offline source to
Parquet/MinIO when `curated` outgrows Postgres or training needs
columnar scans over long history; move materialization to async
(`POST /materialize?async=true` + polling) if a run's window ever
approaches the task timeout; give the feature server its own registry
cache TTL tuning if `feast apply` churn becomes frequent.

## ADR-0006: MLflow as a capped single server, promotion as config + audit table, datasets pinned by `as_of`

**Problem.** Milestone 4 needed (1) an MLflow tracking server and model
registry that fits the memory budget, (2) a definition of "reproducible"
that survives a dataset that grows every five minutes, and (3) a
candidate → champion promotion that is explicit and auditable rather
than a silent alias flip.

**Decision.**
1. **One `mlflow` container** (`infra/mlflow/Dockerfile`, pinned to the
   same `mlflow==3.16.1` the training package resolved), backend store in
   its own `mlflow` database on the shared Postgres (ADR-0004 pattern),
   artifacts in the MinIO `mlflow` bucket **proxied by the server**
   (`--serve-artifacts`, so clients need no MinIO credentials), uvicorn
   with `--workers=2`, `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`,
   `--allowed-hosts` set explicitly, `mem_limit: 1g`.
2. **Training runs in a one-shot `train` container** built from the uv
   workspace (Feast + MLflow + scikit-learn resolved together — no
   isolation needed, unlike Airflow). `amel-train train | promote | show`
   is the whole production interface; no notebook exists.
3. **Reproducibility = same `as_of` + same config → same dataset
   fingerprint → same metrics.** The training frame is "the most recent
   `max_rows` labels with `label_timestamp <= as_of`", joined
   point-in-time through Feast's offline path; `as_of` defaults to now
   and is always logged, so any run can be re-pinned later. The
   fingerprint is a SHA-256 over the sorted (entity_id, event_timestamp,
   target) rows.
4. **Promotion criteria live in config** (`TRAINING_PROMOTION_*`: an
   absolute test-accuracy floor, a test-ROC-AUC floor, and a maximum
   regression against the current champion), the decision is a pure
   function (`evaluate_promotion`), and applying it writes both the
   MLflow alias/tags **and** a row in `ml.model_promotions` (Alembic
   `0004`) with who/why/criteria/both sides' metrics. `--force` is
   allowed but recorded as forced. Serving (Milestone 5) resolves
   `models:/higgs_decision_tree@champion` — never a version number.
5. The sklearn model is logged in MLflow 3's default **skops** format
   with `sklearn.tree._tree.Tree` declared trusted; the declaration is
   stored in the MLmodel flavor config so loaders need nothing extra.

**Reason.**
1. The stock `mlflow server` in 3.x starts 4 gunicorn workers plus a job
   runner and two huey consumers — measured **1,023 MiB, pinned at the
   1 GB cap** on first boot. Nothing in AMEL uses server-side jobs; with
   2 uvicorn workers and jobs off it is 483–534 MiB. `--serve-artifacts`
   keeps MinIO credentials out of every training/inference container.
   MLflow 3's DNS-rebinding guard rejects the in-network `mlflow:5000`
   Host header unless allowed, and that guard only works with uvicorn —
   hence no `--gunicorn-opts`.
2. Reusing the workspace/Docker pattern from Feast keeps one lockfile
   and one build recipe; `TRAINING_MAX_ROWS=200000` keeps a dev run at
   ~36 s and a measured **peak 873 MiB** (3 GB cap); `0` trains on every
   labelled row.
3. "Same code, same seed" is not enough when the source table is
   append-only and live: without `as_of`, two back-to-back runs see
   different rows. Proven: runs v2 and v3 with `as_of=2026-09-19T21:00`
   produced fingerprint `ff72b440727cb949` and bit-identical metrics.
4. The registry's alias is the *current state*; the audit table is the
   *history with reasons*. MLflow model-version tags carry the same
   context so a reviewer sees it in the UI, but tags are mutable and
   unordered — the Postgres row is the record. Criteria as config means
   raising the bar is a config change with a paper trail, not a code
   edit. Proven: v4 (`max_depth=2`, test accuracy 0.6298) was rejected
   with three explicit reasons and `exit 2`; champion stayed v3.
5. skops is safer than pickle for a model that will be loaded by a
   long-running API; declaring exactly one trusted type is the narrow
   exception the tool asks for.

**Tradeoffs.** A single MLflow server is a single point of failure for
training *and* (from Milestone 5) for model resolution at inference
start-up — the inference API must cache the loaded champion and treat
MLflow as needed only for refresh. `--serve-artifacts` routes every
artifact byte through the server. The 200k default under-uses the data;
full-scale runs are one env var away. `as_of` pins the *selection*, not
the *content* of curated rows — an upstream re-curation that changed a
feature value would change the fingerprint, which is the desired
behaviour.

**Future reconsideration trigger.** Add a champion-vs-candidate
evaluation on a fixed, versioned holdout set (not each run's own test
split) once model comparisons matter more than the mechanics; move
promotion behind the platform API + JWT scope `models:promote`
(Milestone 11+) so the agent path and the CLI path share one audited
function.

## ADR-0007: Inference API — cached champion with controlled refresh, Feast over HTTP, persist-then-publish

**Problem.** Milestone 5 needed `apps/inference_api` to serve
`models:/higgs_decision_tree@champion` with predictions that carry model
metadata, take features either raw or from the online store, persist
every prediction, publish it to Kafka — and stay honest about its own
health.

**Decision.**
1. **Model cache with atomic swap.** The champion is loaded once at
   start-up and held in a `ModelCache`; refresh happens only via
   `POST /model/refresh` (shared `X-Admin-Token` until JWT scopes arrive
   with the platform API) or an optional poll
   (`INFERENCE_MODEL_REFRESH_SECONDS`). A refresh loads the new version
   fully before swapping under a lock; a failed load leaves the previous
   model serving and records the error. Same alias → same version is a
   no-op. MLflow client retries are capped (`MAX_RETRIES=2`,
   `TIMEOUT=10`).
2. **Feast over HTTP** (`feast-server /get-online-features`), not the
   Feast SDK — the image carries mlflow + scikit-learn + pandas and
   nothing of Feast's tree.
3. **Persist, then publish.** `ml.predictions` (Alembic `0005`) is
   written first and is the system of record; the `PredictionEvent`
   (`predictions.v1`, shared schema in `amel_common`) is the
   notification. Persistence failure is counted, logged and reported in
   the response (`persisted: false`) — the prediction is still returned.
4. **Probes:** `/health` is "the process can serve or recover" (always
   200 unless the process is wedged); `/ready` is "a model is loaded and
   feast-server, Postgres and Kafka delivery are healthy right now".
   MLflow is deliberately *not* a readiness dependency: it is needed to
   load, not to serve.
5. Column order is pinned to `HIGGS_FEATURE_NAMES`, the same tuple that
   defines the feature view and the training frame; raw requests must
   contain exactly those 28 names.

**Reason.**
1. A registry lookup + artifact download + skops load per request is
   both slow and a hard dependency on MLflow at request time. Proven:
   with MLflow stopped, `/predict/entity` returned 200 and `/ready`
   stayed 200; a refresh during the outage failed in 20 s (was minutes
   with default retries) and left v5 serving; after MLflow returned the
   next refresh cleared the error. "Controlled" is the point — promoting
   a champion (Milestone 4) and *serving* it are two auditable actions;
   the API log shows exactly when the swap happened (`model_swapped
   previous=3 current=5`).
2. Same reasoning as ADR-0005's feature server: dependency isolation,
   one service boundary for online features, and the measured cost is
   ~2 ms per fetch.
3. If the row and the event disagree, the row wins; making the write
   first means an event never refers to a prediction that does not
   exist. Returning `persisted: false` instead of a 500 keeps the
   product (the prediction) available during a database blip while
   making the degradation visible to the caller and to `/ready`.
4. The Milestone 3 lesson applied from day one: every readiness
   dependency is something a prediction *right now* needs.
5. The feature view, the training frame, the signature and the request
   validator all derive from one tuple — that is the train/serve-skew
   guarantee in code. Proven: the same entity through `/predict/entity`
   (Redis) and `/predict/raw` (Postgres values) returned probability
   `0.7061068702290076` both ways.

**Tradeoffs.** A shared admin token is not real auth (documented,
replaced in Milestone 11). Persistence/publishing are synchronous in the
request path (~5.7 ms p50 end-to-end measured, so acceptable; a queue
would decouple them at scale). No batch endpoint. No model warm-up
beyond the initial load. Predictions are persisted with their full
feature vector (audit-friendly, storage-hungry at scale).

**Future reconsideration trigger.** Move refresh behind the platform
API's `models:promote`/`deployments:write` scopes; add a canary
(serve `@candidate` to a fraction of traffic) once there is traffic
worth splitting; make persistence async if p95 latency matters more
than write-before-publish ordering.

## ADR-0008: Observability — one collector, OTLP from every first-party service, spanmetrics, Grafana over Prometheus + Tempo + Loki + Postgres

**Problem.** Milestone 6 needed a single prediction traceable across
service boundaries, logs correlated to traces, metrics in Prometheus and
dashboards in Grafana — on a host with ~8 GB of headroom and a history
of memory crashes.

**Decision.**
1. **One shared `amel_common.telemetry` module.** `configure_telemetry()`
   sets up the tracer and logger providers (OTLP/HTTP to
   `otel-collector:4318`), W3C propagation, and instrumentors for
   FastAPI, SQLAlchemy, httpx and confluent-kafka. Services call it
   once; it is a no-op unless `OTEL_ENABLED=true`, so tests never need
   a collector.
2. **Logs go through stdlib `logging`** (structlog renders, stdlib
   emits) so one OTLP handler ships every line to Loki with
   `trace_id`/`span_id` injected from the active span; stdout keeps the
   same JSON. No log-scraping sidecar.
3. **The API's `X-Trace-Id` *is* the OpenTelemetry trace id** when a
   span is active (else the caller's header, else fresh) — the id in the
   response is the id in Tempo, in Loki and in `ml.predictions`.
4. **feast-server is auto-instrumented** (`opentelemetry-instrument
   feast serve`) so the feature fetch continues the trace into Feast and
   its Redis `HMGET`. **Airflow** uses its native OTel integration (gRPC
   to the collector) for DAG-run/task spans.
5. **Collector pipelines:** traces → Tempo (and a `spanmetrics`
   connector → Prometheus exporter: request rate, errors, duration per
   service/route, derived from spans); logs → Loki's native OTLP
   endpoint. **Prometheus** scrapes the services' existing `/metrics`,
   the collector's spanmetrics, and a `kafka-exporter` for consumer
   lag. **Grafana** is provisioned with Prometheus/Tempo/Loki and a
   **Postgres datasource** so dashboard panels can show pipeline runs,
   predictions per model version and the promotion audit straight from
   the tables. Trace→logs and logs→trace links are provisioned.
6. Kafka consumer spans are **linked**, not parented, to producer spans
   (OTel messaging semantics for batch consumers): a feature event is
   two traces — `higgs.features.v1 send` in the simulator and `recv` in
   the ingestor — joined by a link, with `ingest_batch` spans over the
   DB transaction.
7. Everything is memory-capped and retention is short (Tempo 24 h via
   the 3.x `backend-scheduler` flag, Loki 24 h, Prometheus 2 d):
   collector 256 MB, Tempo 512, Loki 512, Prometheus 512, Grafana 256,
   kafka-exporter 64. Measured steady state ≈ 0.9 GB for all six.

**Reason.**
1–3. Tracing only pays off when every hop propagates the same way and
   the operator can pivot from a response to its trace to its logs
   without translating ids. Proven by `make smoke-tracing`: one id in
   the response header, a 13-span trace across `inference_api` and
   `feast_server` in Tempo, the log line in Loki, the row in Postgres.
4. The feature fetch is the one cross-service hop a prediction makes;
   without instrumenting Feast the trace would stop at an httpx client
   span. Auto-instrumentation costs one command-line prefix.
5. Spanmetrics give RED metrics for services that expose no Prometheus
   endpoint of their own (feast-server, Airflow) for free; the Postgres
   datasource avoids building metrics for facts the database already
   holds.
6. A single poll can return messages from many producers and traces;
   parenting one batch span to one of them would be a lie.
7. Five new always-on containers on this host must be a deliberate,
   measured decision (Milestone 3's lesson); caps make a runaway a
   container exit, not a VM freeze.

**Tradeoffs.** Only first-party Python logs reach Loki (Kafka, Postgres,
MinIO container logs do not — `docker logs` still works; a Docker
log-scraper is the upgrade path). Grafana is anonymous-admin (dev only).
The dashboard is one hand-written JSON; alerting is not configured.
Airflow's exporter ignores `OTEL_EXPORTER_OTLP_PROTOCOL` and needs the
gRPC port. Tempo 3.x has no YAML key for retention.

**Future reconsideration trigger.** Add Grafana Alloy for Docker/
Kubernetes log collection in Milestone 8; alert rules once the platform
event model (Milestone 11) defines what an incident is; OTel metrics
export from the SDK if `/metrics` scraping becomes awkward under
Kubernetes.

## ADR-0009: CI on GitHub Actions with a synthetic-dataset integration job; images tagged by commit SHA; `latest` tags banned

**Problem.** Milestone 7 needed a clean checkout to start with documented
commands, and CI that validates every push and builds deployable images
— without the 2.8 GB HIGGS download and within a 7 GB runner.

**Decision.**
1. **Pinned image tags everywhere.** Every `latest` in Compose became
   the exact version verified on 2026-09-20 (MinIO release tags, Tempo
   3.0.0, Loki 3.7.8, Prometheus v3.14.0, Grafana 13.2.2, kafka-exporter
   v1.10.0). `restart: unless-stopped` on all 16 long-running services;
   healthchecks wherever the image has a shell (Tempo/Loki/collector are
   distroless — documented); non-root users in every first-party image.
2. **A synthetic HIGGS archive** (`scripts/make_synthetic_higgs.py`:
   `HIGGS.csv.gz` inside a zip, no header, label + 28 floats) stands in
   for the real one in CI and in clean-checkout runs; the simulator reads
   it through the same code path (tested).
3. **One workflow, five kinds of job**: quality (ruff, format, mypy,
   pytest), compose config validation, Trivy CRITICAL scan of `uv.lock`
   (`.venv` skipped — Feast's bundled UI lockfile carries JS CVEs we
   don't ship), a matrix build of the 8 first-party images tagged
   `ghcr.io/<repo>/<image>:<sha>` and `:<ref>` (pushed only on `main`/
   tags), and an **integration job** that runs Postgres + Kafka +
   simulator + ingestor on the synthetic archive and executes the
   Milestone 1 smoke test with a reduced threshold. Nothing deploys.
4. **Clean-checkout proof is a real run, not a claim**: fresh `git
   clone`, `make install`, synthetic archive, `make up` under a new
   Compose project name (fresh volumes) with the main stack stopped, then
   the whole path through `make smoke-tracing`.

**Reason.**
1. `latest` moved under us twice already (MinIO off Docker Hub; Tempo 3
   config keys). A pin is a fact; `latest` is a hope.
2. The real dataset is the one thing a fresh machine cannot get
   reliably; the packaging is the only contract the simulator depends on.
3. Runners have ~7 GB; the ingestion slice is ~2.5 GB and finishes in
   ~2 min. Building images in CI is what makes "deployable images from
   main" true; the SHA tag is the traceability the kickoff asks for.
4. The clean run found two real defects the developer's own stack had
   masked: fresh Airflow pauses new DAGs (the trigger sat `queued`
   forever), and span-derived metrics lag ~30 s after the first-ever
   prediction (the tracing smoke raced them).

**Tradeoffs.** CI does not exercise Feast/MLflow/inference (memory and
time on a shared runner); the clean-checkout run does, by hand, per
milestone. GHCR images are built for `linux/amd64` only. Trivy ignores
unfixed CVEs and HIGH severity.

**Future reconsideration trigger.** Add a Kubernetes manifest validation
job in Milestone 8 (`kubeconform`); extend the integration job to
Feast + inference if a larger runner becomes available; add image
scanning (not just lockfile) once images are pulled from GHCR by
Kubernetes.

## ADR-0010: kind next to Compose — only stateless services move, the node is memory-capped, and probes must detect "alive but stuck"

**Problem.** Milestone 8 needed core stateless services on a local
Kubernetes cluster with health probes and configuration separation, on
the same 15 GB host that already runs the 20-container Compose stack.

**Decision.**
1. **kind, one node, on the Compose network.** The cluster is created
   with `KIND_EXPERIMENTAL_DOCKER_NETWORK=amel_default`, so pods resolve
   `postgres`, `kafka:9092`, `redis`, `mlflow`, `otel-collector` by their
   Compose names — no service mirroring, no host networking tricks. The
   node container is capped right after creation
   (`docker update --memory 3g amel-control-plane`).
2. **Four services move, nothing stateful does:** `inference-api`,
   `feast-server`, `source-simulator`, `stream-ingestor` become
   Deployments (+ NodePort Services, an Ingress for the API via
   ingress-nginx at `amel.localtest.me`). Postgres, Kafka, Redis, MinIO,
   MLflow, Airflow and the observability stack stay in Compose.
   `make k8s-up` stops the four Compose counterparts first and points
   Compose's Airflow at the in-cluster feast-server NodePort; `make
   k8s-down` reverses it. `migrate` and `feast apply` run as Jobs.
3. **Configuration separation:** one ConfigMap for non-secret settings,
   one Secret created by the script from environment variables (a
   committed `secret.example.yaml` documents the keys), a
   `simulator-runtime` ConfigMap carrying `HIGGS_START_INDEX` computed at
   deploy time. Images are the same ones Compose builds, loaded with
   `kind load docker-image`.
4. **Probes are the M3/M5 endpoints, plus a stall watchdog.** Startup,
   liveness (`/health`) and readiness (`/ready`) on every Deployment;
   `maxUnavailable: 0` so readiness gates rollouts. The M8 probe test
   found that a *frozen* Postgres (SIGSTOP) makes the ingestor's DB call
   hang forever — thread alive, no error to retry, `/health` 200. Both
   loop services now fail liveness when no progress has been made for
   `stall_timeout_seconds` (120 s default; a paused simulator is exempt).
5. **Head-sample the Kafka-heavy services.** Per-message spans from the
   simulator and ingestor OOM-looped Tempo (113 restarts at 512 MB);
   they now sample 2% (`parentbased_traceidratio`), inference stays at
   100%, Tempo gets 768 MB.
6. CI validates every manifest with `kubeconform` and `kubectl
   kustomize`.

**Reason.**
1. kind is one container (~1.2 GB idle, kube-apiserver alone ~300 MB);
   minikube's docker driver is the same idea with more defaults. The
   Compose-network trick is what makes "move only stateless services"
   cheap: nothing in the cluster needs to know it is talking to Compose.
   Measured: pods resolved and reached `kafka:9092`/`postgres:5432` on
   the first try.
2. Stateful services on a single-node kind cluster would only add
   PersistentVolume ceremony without teaching anything the Compose
   volumes don't; the acceptance names *stateless* services. Net memory
   cost measured: node anon **1.7 GB** with all four pods + ingress-nginx
   (docker stats shows 2.3–2.5 GB because loaded images sit in page
   cache, which is reclaimable); the four Compose containers it replaced
   were ~0.9 GB; host went from ~8.1 to ~8.9 GB used, 6.9 GB available.
3. The same images with different config is the point of the exercise;
   the bad-config rollout test (wrong MLflow URL) proved readiness holds
   the old pod in service while the new one sits `Ready: false` with
   `no model loaded: … nowhere:5000`, then `rollout undo` restores it.
4. "Loop thread alive" was the M3 definition of liveness; a hung
   syscall satisfies it while doing nothing. Progress is the only signal
   that covers both death and hang. Proven: Postgres paused → `503
   consume loop stalled for 67s` → kubelet `Killing … failed liveness
   probe` → 3 restarts during the pause → `Ready: true` and ingesting
   again after unpause.
5. Sampling is the standard lever; 2% of ~200 spans/s keeps the linked
   consumer traces visible for debugging without drowning Tempo. RED
   metrics for those two services are now sampled estimates (documented
   on the dashboard); inference RED stays exact.

**Tradeoffs.** Two runtimes to keep in sync (Compose and k8s manifests
carry the same env keys — a drift risk; the ConfigMap comments point at
Compose). Airflow's `FEAST_SERVER_URL` flips per mode via an env
override. NodePorts reuse the Compose host ports, so the two modes are
mutually exclusive for those four services. Single node → no real
scheduling, no PDBs. Docker page cache inflates the node's apparent
memory. A stalled ingestor is *restarted*, which does not fix a frozen
database — but it makes the outage visible where before it was silent.

**Future reconsideration trigger.** Milestone 10 (HPA/KEDA) will need
metrics-server and more ingestor replicas — re-measure the 3 GB cap
then. If Compose/k8s config drift bites, generate both from one source.
Move the stateful services in only when a multi-node or managed cluster
exists.
