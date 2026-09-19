# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 3 — Feast + Redis feature store.** Complete.

## Completed work (Milestone 3; Milestones 0–2 summarized in MILESTONE_REPORT.md)

- **`ml/feature_repo`** (new uv workspace member `amel-feature-repo`,
  `[tool.uv.workspace] members` now includes `ml/*`): Feast 0.66
  `definitions.py` (entity `entity_id`; feature view `higgs_features`
  with the 28 HIGGS `Float32` fields, `tags={"version": "1"}`, 10-year
  TTL — see the comment for why), `feature_store.yaml` (SQL registry in a
  new `feast` Postgres DB, Postgres offline store with `sslmode:
  disable`, Redis online store), `scripts/materialize.py` (chunked,
  resumable backfill), `scripts/demo_retrieval.py` (acceptance demo),
  tests. `DECISIONS.md` ADR-0005 records the offline-store choice.
- **PostgreSQL**: Alembic `0003` adds VIEW `curated.higgs_features_flat`
  (28 typed `float8` columns generated from `HIGGS_FEATURE_NAMES` over
  the JSONB `features` column); `infra/postgres/init/02-create-feast-db.sql`
  creates the `feast` database.
- **Redis 7** (`redis:7-alpine`, host port 6380, `redis-data` volume) as
  the online store.
- **Compose services**: `feast-apply` (one-shot, default set),
  `feast-server` (always-on `feast serve`, `mem_limit: 1g`,
  host port 6566, healthchecked — ADR-0005 explains why always-on),
  `feast-materialize` and `feast-demo` behind `--profile feast-demo`
  (`make feast-materialize`, `make feast-demo`). All Feast containers
  memory-capped after an unbounded materialize hit 12.1 GB.
- **`update_feature_store` is real**: `higgs_pipeline_tasks.py`
  selects this run's inserted rows by `source_run_id`, splits them into
  ≤25,000-row windows (boundaries computed in SQL), and `POST`s each to
  `feast-server /materialize`; zero rows → no call. Airflow reaches it
  via `FEAST_SERVER_URL`. Feast is *not* installed in the Airflow image.
- **Honest probes** (found necessary mid-milestone, see the incident in
  `LEARNING_LOG.md`): `source_simulator` and `stream_ingestor` now derive
  `/health` (liveness) and `/ready` (readiness) from real producer/
  consumer state and expose it in `/status`; the simulator's loop
  survives transient publish errors; the ingestor treats a rejected
  offset commit as a redelivery, not a crash.
  `KAFKA_MESSAGE_TIMEOUT_MS` (default 30 s) and `HIGGS_START_INDEX`
  added to the simulator.
- **Failure engineering**: `scripts/failure_engineering/
  simulator_producer_wedge.py` (`make failure-simulator-wedge`) pauses
  the broker and asserts the probe transitions and recovery.
- 45 → **69 unit tests**, none requiring infra.

## Milestone 3 acceptance run

Run 2026-09-19 against the live stack after a clean `make down` /
Redis-volume wipe / `make up` (the previous session had been lost to a
WSL2 freeze mid-materialize; nothing from it was trusted).

```
Backfill (make feast-materialize, after redis-cli FLUSHALL → DBSIZE 0):
  36 windows, 167 s, peak container memory 875 MiB (docker stats)
  Redis DBSIZE 884,910 == count(*) curated.higgs_features
  (the unchunked `feast materialize-incremental` before it: OOM-killed at 12.1 GB RSS)

Demo (make feast-demo):
  feature view: higgs_features version=1 features=28
  historical retrieval: 20 rows, 0 with any missing feature
  as-of one day earlier: 20 rows, 0 with leaked (future) features
  online retrieval: 5 entities requested, 5 returned a value
  online == offline for lepton_pt: 5/5
  Milestone 3 demo PASSED

DAG end-to-end (manual__2026-09-19T20:21:09 after new ids started landing):
  update_feature_store_task success — status=materialized windows=3
  rows this run inserted: 51,880 (higgs-000886643 .. higgs-000938837)
  Redis DBSIZE 936,790 == curated count 936,790
  feast-server /get-online-features higgs-000938837:
    lepton_pt 1.029426097869873, m_bb 0.6682522296905518  == Postgres flat view
  entity landed after the run (higgs-000944187): online value None (correctly absent)
  control.pipeline_runs: 31/31 success (13 at session start)

Failure engineering (make failure-simulator-wedge):
  /ready -> 503 after 32 s: "kafka delivery failing: ... Local: Message timed out"
  /health 200 (transient, not fatal — correct); delivery_failures 0 -> 294
  after unpause: /ready 200 in 0 s, rows_read advancing; PASSED

Steady-state memory with everything up: ~5–6 GB of 15 GB
  (feast-server 165 MiB idle / 361 MiB after materialize calls; Redis 495 MB)
```

**All Milestone 3 acceptance criteria verified**: historical
(point-in-time-correct, with a negative leakage check) retrieval and
online materialization + retrieval both work as separate code paths on
real curated data, the scheduled DAG materializes each run's new rows,
and online values equal offline values.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute")

Unchanged from Milestone 0/1/2 (re-verify at the start of Milestone 4) —
see git history for the full table. Additions this milestone:
`redis:7-alpine` (7.4.11), `feast[postgres,redis]==0.66.0` (in the uv
workspace — it resolved cleanly, unlike Airflow), `mermaid-cli`/`mermaid@11`
used only to parse-check the ARCHITECTURE.md diagrams.

## Current known failures / gaps

- `build_training_dataset` full-join scan (carried from Milestone 2;
  ~50 s at ~550k curated rows) — unchanged.
- `source_simulator.ensure_dataset()` download hardening (carried from
  Milestone 1) — unchanged.
- The simulator does not persist its own position: after a restart,
  `HIGGS_START_INDEX` must be set by hand to `max(landed)+1`
  (RUNBOOKS.md) or it replays already-seen ids. Fine for Compose; a
  Kubernetes deployment (Milestone 8) should externalize this.
- Per-run materialization in the DAG is synchronous; a window
  approaching the 10-minute task timeout would need
  `POST /materialize?async=true` + polling (ADR-0005 trigger).
- `curated.higgs_features_flat` is a plain VIEW — JSON extraction is
  recomputed on every offline read. Promote to a materialized view or a
  flat table if historical retrieval becomes slow.
- Feast's Postgres offline store materialization is ~14 KB/row of
  memory; the 25k-row chunk (~875 MiB peak) is tuned for this machine.
- The stream_ingestor's health regression is covered by unit tests but
  has no live failure-engineering script yet (the simulator's does
  double duty: pausing Kafka also exercises the ingestor's rejoin path,
  observed manually). Candidate for the Milestone 7+ failure pass.
- No CI workflow yet (Milestone 7).

## Commands that work today

```bash
make install       # uv sync --all-packages
make lint / fmt / typecheck / test   # all pass, no infra required (69 tests)

make up             # postgres, kafka, minio, redis, airflow, feast-server + one-shots
make logs
make smoke          # Milestone 1 check
make feast-materialize   # Milestone 3 backfill (chunked, resumable)
make feast-demo          # Milestone 3 acceptance demo
make failure-simulator-wedge   # pauses Kafka ~30 s, checks probes, recovers
make down

# Feast (Milestone 3):
curl -s localhost:6566/health
curl -s -X POST localhost:6566/get-online-features -H 'Content-Type: application/json' \
  -d '{"features":["higgs_features:lepton_pt"],"entities":{"entity_id":["higgs-000000001"]}}'
docker exec amel-redis-1 redis-cli DBSIZE      # must equal count(*) of curated.higgs_features
docker exec amel-postgres-1 psql -U amel -d feast -c "SELECT feature_view_name FROM feature_views;"

# Probes (Milestone 3 fix):
curl -s localhost:8001/status | python3 -m json.tool   # simulator: live/ready/producer_health
curl -s localhost:8002/status                          # ingestor

# Restarting the simulator without replaying ids:
MAXID=$(docker exec amel-postgres-1 psql -U amel -d amel -tAc "SELECT max(entity_id) FROM landing.higgs_feature_events")
HIGGS_START_INDEX=$(( 10#${MAXID#higgs-} + 1 )) docker compose -f infra/docker-compose.yml up -d --no-deps source-simulator

# Airflow (Milestone 2), unchanged:
docker exec amel-airflow-1 airflow dags trigger higgs_pipeline
docker exec amel-airflow-1 airflow tasks states-for-dag-run higgs_pipeline <run_id>
```

## Next task

**Milestone 4 — Training + MLflow.**

Acceptance (AMEL_KICKOFF_PROMPT.md "Training" and "MLflow" sections): a
reproducible training package (`ml/training`) using
`DecisionTreeClassifier` that retrieves features through Feast's
*offline* path, with configuration, seeded train/validation/test split,
metrics, confusion matrix, feature importances, model signature, dataset
version, Git SHA, and the feature view version (`higgs_features` v1)
recorded; an MLflow tracking server (Postgres backend, MinIO `mlflow`
bucket for artifacts) logging all of it; registration under
`higgs_decision_tree` with `candidate`/`champion` aliases and explicit
promotion criteria. No notebook required for production training.

Before starting: `make up`, confirm `feast-server` healthy and Redis
`DBSIZE` == curated count, re-verify tool versions. Memory budget: the
MLflow server is one more always-on container (~300 MB); training on
~900k rows × 28 floats is ~200 MB in pandas — fine, but cap the
container.
