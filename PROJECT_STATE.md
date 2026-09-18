# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 1 — PostgreSQL, Kafka, source simulator, stream ingestor.** Complete.

## Completed work

- **uv workspace** (`[tool.uv.workspace]` in root `pyproject.toml`,
  members `libs/*` and `services/*`): `libs/amel_common` (versioned
  Pydantic event schemas, structured JSON logging), `libs/amel_db`
  (SQLAlchemy models + Alembic migrations), `services/source_simulator`,
  `services/stream_ingestor`. `make install` now runs `uv sync
  --all-packages` to pull in the whole workspace (plain `uv sync` only
  syncs what the root project depends on, which is nothing — the root is
  a virtual/non-package project).
- **PostgreSQL**: schemas `control`, `landing`, `curated`, `ml`, `audit`,
  `finops` provisioned; `landing.higgs_feature_events` and
  `landing.higgs_label_events` tables built, with `event_id` as primary
  key (the idempotency mechanism) plus indexes on `entity_id` and the
  timestamp columns. Alembic migration `0001_schemas_and_landing_tables`.
- **Kafka**: KRaft mode (no ZooKeeper), `apache/kafka:3.9.0`, dual
  listeners (`kafka:9092` in-network, `localhost:29092` from the host).
  All 7 topics from the spec created by a one-shot `kafka-init` container
  running `infra/kafka/create_topics.sh` (idempotent — `--if-not-exists`).
- **`services/source_simulator`**: downloads/streams the UCI HIGGS
  dataset (zip never fully extracted or loaded into memory), publishes
  `FeatureEvent`s immediately and `LabelEvent`s after a configurable
  delay (heap-based `DelayedDispatcher`), with configurable rate, burst
  mode, deliberate duplicate generation, and deliberate malformed-payload
  generation (four corruption strategies, see `malformed.py`). `/health`,
  `/ready`, `/status`, `/pause`, `/resume`, `/metrics` (Prometheus).
- **`services/stream_ingestor`**: single consumer group across both
  topics, batched consumption, schema validation against the *same*
  Pydantic models the simulator uses, idempotent bulk upsert (`INSERT ...
  ON CONFLICT (event_id) DO NOTHING ... RETURNING event_id` — the
  `RETURNING` diff is how duplicates are *counted*, not just tolerated),
  Kafka offset commit only after the DB transaction commits, DLQ routing
  for invalid messages (offset committed immediately — see
  LEARNING_LOG.md for why), exponential-backoff retry on transient DB
  failure, and a deliberate process-stop (not a silent skip) if retries
  are exhausted, relying on redelivery-after-restart for recovery.
  `/health`, `/ready`, `/metrics`.
- **Docker Compose** (`infra/docker-compose.yml`): postgres, kafka,
  kafka-init (one-shot), migrate (one-shot, `infra/db/Dockerfile`),
  source-simulator, stream-ingestor — full dependency chain via
  `depends_on` conditions (`service_healthy` / `service_completed_successfully`).
- **`make up`/`down`/`logs`/`migrate`/`smoke`** are now real;
  `scripts/smoke_milestone1.py` drives the acceptance check.
- **Verified directly** (not just argued): malformed events reach the
  DLQ with a specific validation-failure reason; duplicate-delivery
  events are skipped (`count(*) == count(DISTINCT event_id)` holds); a
  `docker kill -s SIGKILL` on `stream_ingestor` mid-processing, followed
  by a restart, recovers with zero duplicate rows (see the two entries in
  `RUNBOOKS.md`). Full acceptance-scale run (100,000+ events against the
  real HIGGS dataset): **see the Milestone 1 acceptance run section
  below**.
- 21 → 25 unit tests added across the four new packages (dataset
  streaming/cycling against a synthetic fixture, malformed-payload
  corruption logic, delayed-dispatcher ordering, schema validation,
  ingestor `_validate()` logic, DB model/schema sanity) — no live infra
  required for `make test`.

## Milestone 1 acceptance run

Run 2026-09-18 against the real UCI HIGGS dataset (downloaded to
`data/raw/higgs.zip`, gitignored) via `make up` + `make smoke`
(`HIGGS_MAX_ROWS=500000 EVENTS_PER_SECOND=2000`):

```
waiting for >= 100,000 feature events (timeout 600s)...
  ... (progress logged every 5s) ...
  feature events persisted so far: 104,357 / 100,000
checking for duplicate event_ids...
  OK: 104,357 rows, 104,357 distinct event_ids — no duplicates persisted
checking malformed events reached the DLQ topics...
  higgs.features.dlq: 253 message(s) observed
  higgs.labels.dlq: 297 message(s) observed

Milestone 1 smoke test PASSED.
```

Immediately after, `stream-ingestor` was `docker kill -s SIGKILL`'d again
(second time this milestone, this time against the real-scale run) and
restarted: `count(*)` and `count(DISTINCT event_id)` on
`landing.higgs_feature_events` matched exactly both before and after
(159,668 / 159,668). Final counts when the stack was brought down for
this commit: 161,649 feature events and 145,619 label events persisted,
all with distinct `event_id`s; simulator counters at that point: 226,209
features published (4,386 as deliberate duplicates, ~900 as deliberate
malformed) against 224,009 rows read from the real dataset.

**All four Milestone 1 acceptance criteria verified against the real
dataset**: 100,000+ events streamed and persisted; duplicates create no
duplicate records; malformed events reach the DLQ; consumer restart is
safe.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute")

Unchanged from Milestone 0 (re-verify at the start of Milestone 2) — see
git history for that table, or re-run `node -v`, `python3 --version`,
`docker --version`, `docker compose version`. One addition this
milestone: `apache/kafka:3.9.0` and `postgres:16-alpine` Docker images.

## Current known failures / gaps

- The real HIGGS dataset zip (~2.8GB) is fetched from
  `archive.ics.uci.edu`, which is slow and unreliable for this
  environment's network path: one attempt stalled at a fixed byte count
  for over an hour, another had its connection reset mid-transfer at
  ~1.3GB, and the server flatly rejects HTTP Range/resume requests
  (`curl: (33) HTTP server does not seem to support byte ranges`) — so a
  failed attempt cannot resume, only restart from zero. What finally
  worked: a single `curl --retry-all-errors --retry 15` invocation (no
  `-C -`) with a stall guard (`--speed-time`/`--speed-limit`).
  `source_simulator.ensure_dataset()` itself (via `requests`) has none of
  this retry hardening yet — it does one `requests.get(..., stream=True)`
  and gives up on any exception. Worth hardening if this dataset is
  fetched somewhere less patient than an interactive session (add retry/
  backoff, or document a faster mirror). Also **discovered and fixed**
  along the way: the real archive's zip contains `HIGGS.csv.gz` (gzip
  *inside* the zip), not a plain `HIGGS.csv` as originally assumed —
  `dataset.py`'s `_find_csv_member`/`stream_rows` now handle both, with a
  regression test for the gzip-inside-zip case
  (`test_stream_rows_reads_gzip_compressed_csv_member`).
- No deliberate Postgres-down or Kafka-down chaos exercise yet (only
  consumer-kill was exercised, twice — once at small scale, once at
  acceptance scale) — good candidates for the Milestone 7+ Failure
  Engineering pass, not required by this milestone's acceptance criteria.
- No CI workflow yet (Milestone 7).

## Commands that work today

```bash
make install       # uv sync --all-packages
make lint / fmt / typecheck / test   # all pass, no infra required

make up             # postgres, kafka, kafka-init, migrate, source-simulator, stream-ingestor
make logs
make smoke          # scripts/smoke_milestone1.py against a running `make up` stack
make down

# manual verification used during this milestone:
docker exec amel-postgres-1 psql -U amel -d amel -c \
  "SELECT count(*), count(DISTINCT event_id) FROM landing.higgs_feature_events;"
docker exec amel-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group stream-ingestor
```

## Next task

**Milestone 2 — Airflow + MinIO + bronze/silver/gold processing.**

Acceptance: a scheduled DAG processes newly arrived `landing` data
idempotently into `curated` via MinIO bronze/silver/gold Parquet.

Concretely, in order (see `AMEL_KICKOFF_PROMPT.md`'s Airflow/MinIO/Data
Validation sections for full detail):
1. Add MinIO to `infra/docker-compose.yml`; buckets/prefixes
   `bronze/higgs`, `silver/higgs`, `gold/training`.
2. Add Airflow (scheduler + webserver, or a lightweight standalone
   variant — decide and record in `DECISIONS.md`) to Compose.
3. DAG: `determine_high_watermark → extract_new_records → write_bronze →
   validate → transform → write_silver → update_curated_tables →
   build_training_dataset → update_feature_store (stub until Milestone
   3) → emit_pipeline_metadata`. Idempotent re-runs, retries, retry
   delay, timeouts, XCom for small metadata only, failure callbacks.
4. Data validation layer (Pandera or equivalent) producing observable
   validation reports for schema/type/null/target/bounds/duplicate-id/
   timestamp checks.
5. `curated.higgs_features`, `curated.higgs_labels`,
   `curated.training_records` tables (new Alembic migration in
   `libs/amel_db`).
6. Document watermark/checkpoint storage choice and idempotent-DAG-rerun
   reasoning in `LEARNING_LOG.md`.
7. Full Definition of Done pass, commit `feat(milestone-2): ...`, push,
   tag `milestone-2`.
