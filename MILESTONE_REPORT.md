# Milestone Report

One section per completed milestone: what was built, the acceptance
criteria and *how they were proven* (measured numbers from the actual
run, never claims), key decisions, bugs found and fixed, known gaps.
Updated as part of every milestone's Definition of Done. `PROJECT_STATE.md`
holds only the current milestone in detail; this file keeps the history.
Longer narrative and concepts: `LEARNING_LOG.md`. Bug write-ups:
`MISTAKES.md`. Operational procedures: `RUNBOOKS.md`. Decisions:
`DECISIONS.md`.

## Milestone 0 — Repository skeleton, tooling, GitHub remote (2026-09-18, tag `milestone-0`, commit `f723e63`)

**Built.** uv workspace (`pyproject.toml`, `[tool.uv] package = false`),
Python pinned to 3.12 via `uv python install` + `.python-version`
(system Python is 3.14 and incompatible with planned dependencies —
ADR-0001); ruff, mypy, pytest configured; `Makefile` with real
`install/lint/fmt/typecheck/test` and stubbed infra targets;
`.gitignore`, `.env.example`; the documentation set (`README`,
`ARCHITECTURE`, `DECISIONS`, `SECURITY`, `RUNBOOKS`, `LEARNING_LOG`,
`PROJECT_STATE`); public GitHub remote.

**Acceptance and proof.** `make lint`, `make typecheck`, `make test`
pass (2 environment tests: interpreter is 3.12; `ssl`/`sqlite3`/`bz2`/
`lzma` import). Verified tool versions recorded: Python 3.12.14 (uv
0.12.16), Node v22.23.2, Docker 29.7.2 / Compose v5.4.0, git 2.53.0,
gh 2.46.0.

**Decisions.** ADR-0001 (Python 3.12 via uv, not pyenv/deadsnakes —
no passwordless sudo, no build toolchain); ADR-0002 (monorepo, uv
workspace, with the anticipated trigger for isolating strict-pinned
tools like Airflow).

**Bugs.** System Python too new for the dependency set (MISTAKES.md).

**Gaps at close.** No runtime services; no CI (planned Milestone 7).

## Milestone 1 — Kafka + PostgreSQL streaming ingestion (2026-09-18, tag `milestone-1`, commit `48bc1da`)

**Built.** `libs/amel_common` (versioned Pydantic `FeatureEvent`/
`LabelEvent`, structured logging), `libs/amel_db` (SQLAlchemy models,
Alembic `0001`: schemas `control/landing/curated/ml/audit/finops`,
`landing.higgs_feature_events`/`higgs_label_events` with `event_id` PK),
Kafka in KRaft mode (`apache/kafka:3.9.0`, 7 topics incl. DLQs via a
one-shot `kafka-init`), `services/source_simulator` (streams the 2.8 GB
HIGGS zip row by row, delayed labels via a heap dispatcher, configurable
duplicate/malformed injection, burst mode, HTTP control/metrics),
`services/stream_ingestor` (one consumer group over both topics,
batched, Pydantic validation, `INSERT ... ON CONFLICT (event_id) DO
NOTHING ... RETURNING` upsert, offsets committed only after the DB
transaction, DLQ routing, exponential-backoff DB retry). Compose stack
with health-gated `depends_on`. `scripts/smoke_milestone1.py`.

**Acceptance and proof** (`make smoke`, `HIGGS_MAX_ROWS=500000
EVENTS_PER_SECOND=2000`, real dataset):
- 100,000+ events streamed and persisted: **104,357** feature events at
  smoke-pass time; 161,649 features / 145,619 labels at shutdown.
- Duplicates create no duplicate rows: `count(*) == count(DISTINCT
  event_id)` — 104,357 / 104,357; simulator had deliberately published
  4,386 duplicates by shutdown.
- Malformed events reach the DLQ: **253** on `higgs.features.dlq`,
  **297** on `higgs.labels.dlq` (~900 injected).
- Consumer restart is safe: `docker kill -s SIGKILL` mid-batch, restart
  → 159,668 / 159,668 rows before and after, zero duplicates.
- 24 unit tests, no infra.

**Decisions.** ADR-0003 (KRaft, no ZooKeeper); DLQ offsets committed
immediately (never poison-pill a partition); process stops on exhausted
DB retries rather than dropping a batch.

**Bugs found and fixed.** HIGGS download stalls/resets and the server
rejects Range requests (worked around with `curl --retry-all-errors` +
stall guard); the archive is `HIGGS.csv.gz` *inside* the zip, not a
plain CSV (`dataset.py` handles both, regression test added).

**Gaps at close.** `ensure_dataset()` has no retry hardening; no
Postgres-down/Kafka-down chaos exercise; entity ids restart from 0 on
every simulator boot (not recognized as a gap until Milestone 3).

## Milestone 2 — Airflow + MinIO bronze/silver/gold batch pipeline (2026-09-18, tag `milestone-2`, commit `de4f67a`)

**Built.** `libs/amel_lake` (MinIO/S3 client, deterministic object keys,
Pandera validation with row quarantine, watermark read/monotonic
advance); Alembic `0002` (`curated.higgs_features`, `curated.higgs_labels`,
`curated.training_records`, `control.pipeline_watermarks`,
`control.pipeline_runs`, index on `landing.*.ingested_at`); MinIO
(`quay.io/minio`) with `bronze/silver/gold/pipeline/mlflow` buckets;
Airflow 3.3.2 `standalone`/`LocalExecutor` in its own image with its
own Postgres database (ADR-0004); DAG `higgs_pipeline` (10 tasks,
`*/5 * * * *`, retries, 10-min task timeout, failure callback) with all
business logic in Airflow-free `higgs_pipeline_tasks.py`.

**Acceptance and proof** (2026-09-18, live growing landing tables):
- **10/10 DAG runs `success`** in `control.pipeline_runs` (including
  retries after each bug fix and one legitimate zero-row window).
- Idempotent curation: `curated.higgs_features` 884,910 rows / 884,910
  distinct `event_id`; `higgs_labels` 880,628 / 880,628;
  `training_records` 878,903 / 878,903 distinct `entity_id`.
- Watermarks advanced monotonically to `2026-09-18 20:28:16` across
  every run; a deliberately wide re-extraction (after the watermark bug
  below) produced **zero** duplicate curated rows.
- MinIO holds bronze/silver Parquet for every run, gold Parquet for runs
  with new training records, a validation report JSON per run.
- 45 unit tests, no infra.

**Decisions.** ADR-0004 (Airflow: own image, own DB, `standalone`);
medallion layout keyed by `run_id`; join training data against
*accumulated* curated state (late labels), not the run delta.

**Bugs found and fixed.** `KeyError: 'data_interval_end'` on manual
triggers (`_upper_bound()` fallback); Postgres 65,535 bind-parameter cap
at ~48k-row backlog (chunk upserts at 2,000); watermark regression under
concurrent retries (`GREATEST`); `onupdate` never firing through Core
`INSERT ... ON CONFLICT` (explicit `SET`); MinIO images moved off Docker
Hub; a host-port collision with another project; a root-owned named
volume vs a non-root Airflow user.

**Gaps at close.** `build_training_dataset` full-join scan grows with
curated size (~50 s at 550k rows); no MinIO-down/scheduler-down
exercise; `update_feature_store` is a no-op stub.

## Milestone 3 — Feast + Redis feature store (2026-09-19, tag `milestone-3`)

**Built.** `ml/feature_repo` (Feast 0.66: `entity_id` entity,
`higgs_features` view with 28 `Float32` fields, `tags={"version":"1"}`;
SQL registry in a new `feast` Postgres DB; Postgres offline store over
the new VIEW `curated.higgs_features_flat` (Alembic `0003`); Redis 7
online store); `scripts/materialize.py` (chunked, resumable backfill);
`scripts/demo_retrieval.py`; Compose services `feast-apply`,
`feast-server` (always-on `feast serve`, 1 GB cap), `feast-materialize`
/ `feast-demo` (profile). `update_feature_store` in the DAG is real:
this run's rows by `source_run_id` → ≤25k-row windows → `POST
feast-server /materialize`. Found necessary along the way: honest
liveness/readiness in `source_simulator` and `stream_ingestor`,
`HIGGS_START_INDEX`, `KAFKA_MESSAGE_TIMEOUT_MS`, and the first
failure-engineering script (`make failure-simulator-wedge`).
Standing deliverables added: this file; Mermaid diagrams in
`ARCHITECTURE.md`.

**Acceptance and proof** (2026-09-19; stack brought down, Redis volume
deleted, brought up clean; nothing from the crashed session trusted):
- Historical (point-in-time) retrieval: entity df of 20 rows from
  `curated.higgs_labels` → **20/20 rows with all 28 features**; the same
  entities as-of one day earlier → **0/20 with any feature** (no future
  leakage).
- Online materialization + retrieval: backfill of 884,910 entities in
  **36 windows, 167 s, peak 875 MiB** (`FLUSHALL` → `DBSIZE 0` verified
  immediately before); `DBSIZE` == curated count after. Online lookup
  5/5 entities; **online == offline (`lepton_pt`) 5/5**.
- Scheduled path: DAG run `manual__2026-09-19T20:21:09` inserted
  **51,880** new curated rows, `update_feature_store` → `materialized,
  windows=3`; `DBSIZE` 936,790 == curated 936,790; newest entity
  `higgs-000938837` via `feast-server /get-online-features` returned
  `lepton_pt 1.029426097869873, m_bb 0.6682522296905518` == Postgres;
  an entity landed after the run returned `None` online. 31/31 pipeline
  runs `success` by session end.
- Feature versioning: `tags.version=1` printed by the demo and asserted
  by a test; registry keeps `feature_view_version_history`.
- Probes/failure engineering: broker paused → `/ready` **503 after
  32 s** naming the Kafka error, **294** delivery failures counted,
  `/health` stayed 200 (transient), `/ready` back to 200 **0 s** after
  unpause, rows advancing. Ingestor rejoin after group eviction observed
  live (backlog of ~870k mostly-duplicate messages drained in ~3 min).
- Memory: `feast-server` 165 MiB idle / 361 MiB after materialize
  calls; Redis 495 MB at 937k entities; whole stack ~5–6 GB of 15 GB.
- 69 unit tests (45 → 69), `ruff`/`mypy` clean.

**Decisions.** ADR-0005: Postgres offline store + VIEW (not Parquet),
SQL registry, always-on capped feature server (not Feast-in-Airflow,
not Docker-socket-from-Airflow), chunked materialization everywhere.

**Bugs found and fixed.** (1) `feast materialize` **OOM-killed at
12.1 GB RSS** over 885k rows — the probable cause of both earlier WSL2
freezes; fixed by chunking + container caps. (2) Simulator producer
wedge: loop thread died during the broker stall, `/health` 200 for an
hour; fixed with real probe state, loop error handling, 30 s message
timeout; regression script added. (3) Ingestor loop killed by
`UNKNOWN_MEMBER_ID` on commit, `/health` 200 with ~870k lag and no
group members; fixed — rejected commit = redelivery, loop death =
liveness failure. (4) Simulator restart replayed 885k ids;
`HIGGS_START_INDEX`. (5) Feast Postgres offline store `sslmode=require`
default vs a non-TLS Postgres. (6) Migration `0003` was future-dated.

**Known gaps at close.** Simulator position is manual on restart;
per-run materialization is synchronous (async + polling if windows grow);
`higgs_features_flat` is a plain VIEW; ~14 KB/row materialization memory
is tuned by chunk size for this machine; no dedicated ingestor
failure-engineering script yet; Milestone 1–2 carry-overs unchanged
(download hardening, training-dataset scan growth); no CI.

## Milestone 4 — Training package + MLflow (2026-09-19, tag `milestone-4`)

**Built.** `ml/training` (`amel-train train | promote | show`): config
→ Feast offline retrieval pinned by `as_of` → seeded stratified split →
`DecisionTreeClassifier` → metrics/confusion/importances/signature →
MLflow run + registered version aliased `candidate`; `promote` applies
config-driven criteria and writes an `ml.model_promotions` audit row
(Alembic `0004`). MLflow 3.16.1 server (Postgres `mlflow` DB, MinIO
artifacts proxied, 2 uvicorn workers, jobs off, 1 GB cap) and a `train`
one-shot container (3 GB cap).

**Acceptance and proof.**
- Reproducible: two runs with `as_of=2026-09-19T21:00:00` → versions 2
  and 3, same fingerprint `ff72b440727cb949`, **all 13 metrics
  identical** (test accuracy 0.689787603526583, ROC-AUC
  0.7585938300843371), 199,623 rows, ~36 s each, **peak 873 MiB**.
- Registered: `higgs_decision_tree` v1–v4; each version tagged with git
  SHA `a70d233`, fingerprint, `higgs_features:v1`, test metrics; 11
  artifacts + model + dataset input per run; 82 objects in MinIO.
- Promotion: v3 → champion (**approved**, reasons logged, audit row 1);
  v4 (`max_depth=2`, accuracy 0.6298) → **rejected** with three reasons,
  exit 2, champion unchanged. `make model-show` → candidate 4 /
  champion 3.
- MLflow footprint: 1,023 MiB with defaults → 483–534 MiB after tuning.
- 80 unit tests, ruff/mypy clean.

**Decisions.** ADR-0006 (single capped MLflow server with proxied
artifacts; `as_of`-pinned datasets + content fingerprint as the
definition of reproducible; criteria-as-config + pure decision +
Postgres audit row; skops with one trusted type).

**Bugs found and fixed.** MLflow at its memory cap on boot (4 workers +
jobs subsystem); MLflow 3 `403 Invalid Host header` (`--allowed-hosts`,
uvicorn-only); skops untrusted `sklearn.tree._tree.Tree`; blank Compose
env vars parsed as bad datetimes; `mlflow` DB missing on an existing
Postgres volume (init scripts are first-boot-only).

**Known gaps at close.** Comparison on each run's own test split rather
than a fixed holdout; 200k-row default, full-scale untimed; `train`
rebuilds each invocation; MLflow single point of failure for model
resolution (inference must cache); manual DB creation on existing
volumes; earlier carry-overs unchanged.

## Milestone 5 — FastAPI inference (2026-09-19, tag `milestone-5`)

**Built.** `apps/inference_api`: cached champion (`ModelCache`, atomic
swap, controlled refresh via `POST /model/refresh` + optional poll),
`POST /predict/raw` (validated 28 features) and
`POST /predict/entity/{id}` (feast-server online features), persistence
to `ml.predictions` (Alembic `0005`) before publishing a
`PredictionEvent` to `predictions.v1`, `/model`, `/metrics`, honest
`/health` + `/ready`. Compose service on :8003, 1 GB cap.

**Acceptance and proof.**
- Loads the champion at start-up (`model_swapped previous=null
  current=3`); `/model` returns version, run id, git SHA, dataset
  fingerprint, feature view.
- Predictions carry model metadata + `prediction_id` + `trace_id`;
  entity path and raw path agree to the last digit
  (`0.7061068702290076`) for the same entity — online == offline.
- **p50 5.7 ms / p95 6.8 ms** over 200 sequential entity predictions
  including feature fetch, DB write and Kafka publish; 203 rows in
  `ml.predictions`, matching events on `predictions.v1`.
- Controlled refresh proven live: promotion alone left v3 serving;
  refresh without token 401; with token `3 → 5`, next prediction from
  v5.
- MLflow stopped: predictions 200, `/ready` 200, refresh fails in
  **20 s** (vs ~3 min before capping client retries), keeps serving;
  error clears after recovery.
- 404 on unknown entity, 422 on malformed raw input. ~246 MiB RSS.
- 89 unit tests (80 → 89), ruff/mypy clean.

**Decisions.** ADR-0007 (cache + controlled refresh; Feast over HTTP;
persist-then-publish; readiness excludes the registry; one column-order
source of truth).

**Bugs found and fixed.** FastAPI dependency resolved as a query param
under postponed annotations (closure-scoped dependency); refresh during
an MLflow outage blocked ~3 min (client retries capped); stale
`last_error` after a successful no-op refresh.

**Known gaps at close.** Shared admin token until JWT (M11);
synchronous persist/publish; no batch or canary endpoints; no
`ml.model_metadata` mirror; earlier carry-overs.

## Milestone 6 — OpenTelemetry and observability stack (2026-09-20, tag `milestone-6`)

**Built.** `amel_common.telemetry` + trace-correlated logging; OTel
instrumentation in inference_api, source_simulator, stream_ingestor,
the DB engine, feast-server (auto) and Airflow (native); OTel Collector
→ Tempo / Loki / spanmetrics → Prometheus (+ kafka-exporter); Grafana
with Prometheus, Tempo, Loki and Postgres datasources, trace↔log links
and an 11-panel dashboard; `make smoke-tracing`.

**Acceptance and proof.** `make smoke-tracing`: one prediction →
trace `90eb3fe2…` returned as `X-Trace-Id`, **13 spans across
`inference_api` and `feast_server`** in Tempo (server → httpx client →
feast server → Redis `HMGET`; `INSERT amel`; `predictions.v1 send`),
**1 Loki log line** with the id, **1 `ml.predictions` row** with the id,
6 Prometheus targets up, span-derived RED metrics for 5 services,
consumer-lag metric present. Airflow DAG-run traces (19.3 s, 444.5 s)
in Tempo. Observability footprint **≈ 0.9 GB** (all capped); whole
stack ≈ 9.0 GB of 15. 89 unit tests, ruff/mypy clean.

**Decisions.** ADR-0008 (single collector; logs via stdlib → OTLP;
response id = trace id; feast-server auto-instrumented; spanmetrics;
Postgres as a Grafana datasource; linked consumer spans; caps and 24 h
retention).

**Bugs found and fixed.** Tempo 3 retention keys (CLI flag now); Airflow
speaks gRPC regardless of protocol env (port 4317); API trace id was a
minted uuid, not the OTel id; expectation of one trace per Kafka
message (semantics, documented).

**Known gaps at close.** Third-party container logs not in Loki; no
alert rules; anonymous Grafana; Redis growth with the simulator; earlier
carry-overs.

## Milestone 7 — Docker hardening + CI/CD (2026-09-20, tag `milestone-7`)

**Built.** Pinned image tags, restart policies, healthchecks, non-root
users in every first-party image, `.dockerignore`, DAGs unpaused at
creation; `scripts/make_synthetic_higgs.py`; `.github/workflows/ci.yml`
with quality, compose-config, Trivy, 8 SHA-tagged image builds to GHCR
and an ingestion integration job on the synthetic archive.

**Acceptance and proof.**
- **Clean checkout**: fresh clone + fresh volumes → `make up` in
  2 m 41 s, 20 containers, init scripts created all three extra
  databases, Alembic at 0005; M1 smoke 5,391/5,391 rows; DAG curated
  78,432 rows and materialized 4 windows; Feast demo passed; trained
  v1 (0.7879 on synthetic data), promoted, refreshed → `/ready` 200;
  tracing smoke passed; ≈ 7.0 GiB peak.
- **GitHub Actions**: run `35531470319` on `main` **success** in 3 m
  07 s — 12 jobs; Trivy 0 CRITICAL in `uv.lock`; integration job M1
  smoke 5,738 rows / 0 duplicates / DLQ 26 + 40, probes 200.
- 90 unit tests, ruff/mypy clean.

**Decisions.** ADR-0009 (no `latest`; synthetic dataset as the CI
contract; SHA-tagged images; real clean-checkout run as the proof).

**Bugs found and fixed.** Fresh Airflow pauses new DAGs; tracing smoke
raced span metrics; two stale GitHub Action pins; Trivy scanning
Feast's bundled UI lockfile in `.venv`.

**Known gaps at close.** CI integration covers ingestion only; amd64
only; lockfile-only scanning; no in-image healthcheck for distroless
images; clean-checkout run is manual.
