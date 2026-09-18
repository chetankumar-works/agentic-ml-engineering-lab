# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 2 — Airflow + MinIO + bronze/silver/gold batch pipeline.** Complete.

## Completed work

- **`libs/amel_lake`** (new uv workspace member): MinIO/S3 client
  (`object_store.py`), deterministic bronze/silver/gold/validation-report
  object keys (`keys.py`), Pandera-based validation with a
  quarantine-invalid-rows pattern (`validation.py`), watermark read/
  monotonic-advance (`watermark.py`).
- **PostgreSQL**: `curated.higgs_features`, `curated.higgs_labels`,
  `curated.training_records`, `control.pipeline_watermarks`,
  `control.pipeline_runs` (Alembic `0002_curated_and_control_pipeline_tables`,
  which also indexes `landing.*.ingested_at` for the watermark queries).
- **MinIO**: `bronze`/`silver`/`gold`/`pipeline`/`mlflow` buckets,
  provisioned by a one-shot `minio-init` container
  (`infra/minio/create_buckets.sh`, idempotent).
- **Airflow 3.3.2** (`infra/airflow/Dockerfile`, its own image outside
  the uv workspace — see `DECISIONS.md` ADR-0004), running
  `airflow standalone` with `LocalExecutor`, its metadata DB as a second
  database (`airflow`) on the same Postgres instance. DAG `higgs_pipeline`
  (`infra/airflow/dags/`): `determine_high_watermark → extract_new_records
  → write_bronze → validate → transform → write_silver →
  update_curated_tables → build_training_dataset → update_feature_store
  (stub — Feast is Milestone 3) → emit_pipeline_metadata`, with retries,
  a 10-minute per-task timeout, and a DAG-level `on_failure_callback`
  that writes a `control.pipeline_runs` failure row.
- Business logic (`higgs_pipeline_tasks.py`) is Airflow-independent and
  unit-tested in the ordinary uv workspace venv; `higgs_pipeline.py` is
  the thin Airflow wrapper (`@task`, XCom, context, retries).
- **Idempotency**: bronze/silver/gold objects keyed by `run_id`
  (re-running overwrites, never duplicates); curated upserts are
  `ON CONFLICT DO NOTHING`; the watermark only advances in the same
  transaction as the curated upsert it gates, and only ever *forward*
  (`GREATEST`, see the bug below).
- **Three real bugs found and fixed during the acceptance run** (full
  writeup in `LEARNING_LOG.md`'s Milestone 2 entry, runbook entries in
  `RUNBOOKS.md`):
  1. `KeyError: 'data_interval_end'` on manually-triggered DAG runs (no
     data interval exists for those) — fixed with an `_upper_bound()`
     fallback to `dag_run.run_after`.
  2. `psycopg.OperationalError: ... between 0 and 65535` — a bulk upsert
     of one row per extracted record blew Postgres's bind-parameter cap
     at real backlog scale — fixed by chunking every upsert at 2,000 rows.
  3. The watermark could silently *regress* under concurrent/out-of-order
     DAG run retries (unconditional overwrite) — fixed with
     `SET watermark = GREATEST(current, new)`; verified the fix advances
     correctly and that the resulting wide re-extraction window produced
     zero duplicate curated rows (idempotency absorbed the metadata bug
     cleanly).
- 44 → 45 unit tests (up from Milestone 1's 24): dataset/validation/
  watermark/key-building logic in `amel_lake`, DAG task logic in
  `higgs_pipeline_tasks.py` — none require live infra.

## Milestone 2 acceptance run

Run 2026-09-18 against the live, continuously-growing landing tables
(source_simulator + stream_ingestor running throughout) via `make up`
plus manually triggering `higgs_pipeline` (`airflow dags trigger
higgs_pipeline`) both as ad-hoc runs and via its `*/5 * * * *` schedule.

**10 DAG runs, all `status='success'`** in `control.pipeline_runs` by
the end of the session (including runs that initially hit the three bugs
above and succeeded on retry after each fix shipped, and one run that
legitimately processed a zero-row window with no errors).

Final state when the stack was brought down for this commit:

```
curated.higgs_features:   884,910 rows, 884,910 distinct event_id
curated.higgs_labels:     880,628 rows, 880,628 distinct event_id
curated.training_records: 878,903 rows, 878,903 distinct entity_id
control.pipeline_watermarks: higgs_features / higgs_labels both at
  2026-09-18 20:28:16.599563, advancing monotonically across every run
control.pipeline_runs: 10/10 status='success'
MinIO: bronze/silver objects for every run (higgs/{features,labels}/
  dt=.../run_id=....parquet), gold/training/run_id=....parquet for runs
  that added new training records, pipeline/artifacts/validation_reports/
  run_id=....json for every run
```

**All Milestone 2 acceptance criteria verified against real, live data**:
the scheduled DAG processes newly-arrived `landing` data idempotently
into `curated` via MinIO bronze/silver/gold Parquet — including under
adverse conditions (concurrent retries, a stale/regressed watermark
forcing a wide re-extraction) that came up organically during the run,
not from a contrived test.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute")

Unchanged from Milestone 0/1 (re-verify at the start of Milestone 3) —
see git history for the full table. Additions this milestone:
`apache/airflow:3.3.2-python3.12`, `quay.io/minio/minio:latest`,
`quay.io/minio/mc:latest` (MinIO moved off Docker Hub to Quay —
`minio/minio`/`minio/mc` no longer resolve there).

## Current known failures / gaps

- `build_training_dataset` joins the *full* `curated.higgs_features`/
  `curated.higgs_labels` against `curated.training_records` (LEFT JOIN
  WHERE NULL) every single run — correct, but the scan grows with
  curated table size (observed ~50s at ~550k curated rows). Documented
  as a known limitation in the task's own docstring; revisit with
  incremental materialization if/when this becomes the pipeline's
  bottleneck, not before.
- `source_simulator.ensure_dataset()` still has no download retry/
  hardening (carried over from Milestone 1's known gaps) — not touched
  this milestone.
- No deliberate MinIO-down or Airflow-scheduler-down chaos exercise yet
  — good candidate for the Milestone 7+ Failure Engineering pass.
- No CI workflow yet (Milestone 7).

## Commands that work today

```bash
make install       # uv sync --all-packages
make lint / fmt / typecheck / test   # all pass, no infra required

make up             # postgres, kafka, minio, airflow, kafka-init, minio-init,
                     # migrate, source-simulator, stream-ingestor
make logs
make smoke          # Milestone 1 check (100k+ events, dedup, DLQ)
make down

# Airflow (Milestone 2):
docker exec amel-airflow-1 airflow dags trigger higgs_pipeline
docker exec amel-airflow-1 airflow tasks states-for-dag-run higgs_pipeline <run_id>
docker exec amel-airflow-1 airflow dags list-import-errors
# UI: http://localhost:8080 (SimpleAuthManager; admin password printed to
# the airflow container's logs on first boot)

# manual verification used during this milestone:
docker exec amel-postgres-1 psql -U amel -d amel -c \
  "SELECT count(*), count(DISTINCT event_id) FROM curated.higgs_features;"
docker exec amel-postgres-1 psql -U amel -d amel -c \
  "SELECT * FROM control.pipeline_watermarks;"
docker run --rm --network amel_default --entrypoint sh quay.io/minio/mc:latest -c \
  "mc alias set local http://minio:9000 amel amel_dev_password && mc ls -r local/bronze"
```

## Next task

**Milestone 3 — Feast + Redis.**

Acceptance: historical features can be retrieved and online
materialization demonstrated.

Concretely, in order (see `AMEL_KICKOFF_PROMPT.md`'s Feature Store
section for full detail):
1. Add Redis to `infra/docker-compose.yml` as the online store.
2. Build `ml/feature_repo` — Feast entity/feature-view definitions over
   `curated.higgs_features`/`curated.higgs_labels` (Postgres) or the
   `silver`/`gold` Parquet in MinIO as the offline source (decide which
   and record the reasoning in `DECISIONS.md` — Feast's Postgres offline
   store vs. a file/Parquet offline store have different tradeoffs worth
   being explicit about).
3. Demonstrate historical (point-in-time-correct) feature retrieval for
   training, and online feature materialization + retrieval for
   inference, as two clearly separate, working code paths.
4. Replace `update_feature_store`'s no-op stub in `higgs_pipeline.py`
   with a real Feast materialization call, now that there's a feature
   store to materialize into.
5. Document what a feature store solves (point-in-time correctness,
   train/serve skew prevention, online/offline consistency) and what it
   does *not* solve, in `LEARNING_LOG.md`.
6. Full Definition of Done pass, commit `feat(milestone-3): ...`, push,
   tag `milestone-3`.
