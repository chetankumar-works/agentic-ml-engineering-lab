# Runbooks

Operational runbooks for AMEL failure scenarios. Each entry follows the
same shape so the Incident Agent (Milestone 12+) can retrieve and reason
about them, and so a human operator can act on them without other
context:

```
## <failure scenario>
- Symptom: what's observably wrong (metric, log, error, alert)
- Likely causes: ranked list
- Diagnosis steps: exact commands/queries to run
- Recovery: exact steps to restore service
- Data-loss implications: what, if anything, is unrecoverable
- Prevention/follow-up: what to fix so it doesn't recur
```

Entries below are added only as their milestone is built and the failure
is actually, deliberately exercised — never written speculatively ahead
of the system existing. Remaining exercises from `AMEL_KICKOFF_PROMPT.md`
(stop Postgres, stop Kafka mid-flight, remove the MLflow model, API
timeouts, pod crashes, Kafka backlog) get entries as their milestones
land.

## stream_ingestor killed mid-batch (SIGKILL)

Exercised 2026-09-18 against a live `make up` stack: `docker kill -s
SIGKILL amel-stream-ingestor-1` while the simulator kept publishing at
500 events/s, then the container was restarted.

- **Symptom**: `/health` on the ingestor stops responding; Kafka consumer
  group shows the member as still "active" for a while (session timeout,
  ~28s observed) before the group coordinator marks it dead;
  `landing.higgs_feature_events` row count stops increasing.
- **Likely causes**: OOM kill, node eviction, process crash, deploy
  rollout — anything that ends the process without a clean shutdown.
- **Diagnosis steps**:
  - `docker compose -f infra/docker-compose.yml ps stream-ingestor` — is
    it running?
  - `SELECT count(*) FROM landing.higgs_feature_events;` polled over
    time — is it flat?
  - `docker exec <kafka> /opt/kafka/bin/kafka-consumer-groups.sh
    --bootstrap-server localhost:9092 --describe --group
    stream-ingestor` — growing LAG confirms the consumer isn't making
    progress.
- **Recovery**: restart the container (`docker compose up -d
  stream-ingestor`, or let a restart policy/orchestrator do it). No
  manual intervention needed beyond that — this is the scenario the
  idempotent-sink design exists for.
- **What was actually observed on restart**: the new process took ~28s
  to get a partition assignment (waiting out the dead member's session
  timeout — expected Kafka rebalance behavior, not a bug). It then
  resumed from the last *committed* offset, which was one batch behind
  the last *persisted* row (the batch that was DB-committed but whose
  Kafka offset commit never ran before the kill). That batch's rows were
  therefore refetched and correctly skipped by `ON CONFLICT DO NOTHING`
  (`duplicate_events_skipped` in the logs), then normal `persisted`
  processing resumed and lag drained back to near zero. Verified
  directly: `SELECT count(*), count(DISTINCT event_id) FROM
  landing.higgs_feature_events` — equal before and after (61,904 /
  61,904), confirming zero duplicate rows despite the redelivery.
- **Data-loss implications**: none. At-least-once delivery + idempotent
  sink means a crash between DB commit and offset commit causes
  redelivery, not loss — see LEARNING_LOG.md's Milestone 1 entry for the
  full argument.
- **Prevention/follow-up**: none needed — this worked as designed. If
  the gap between DB commit and offset commit needs to shrink (e.g. to
  reduce redelivered-batch size after a crash), lower `batch_size`/
  `batch_timeout_seconds`, trading off some throughput.

## Malformed events (deliberately injected by source_simulator)

Exercised continuously during Milestone 1 development
(`INVALID_EVENT_RATE=0.02` in a dry run over ~60k events).

- **Symptom**: `amel_ingestor_messages_total{outcome="dlq"}` increments;
  `higgs.features.dlq` / `higgs.labels.dlq` receive messages.
- **Likely causes**: upstream schema drift, a buggy producer, wire
  corruption — modeled here by `source_simulator`'s deliberate
  `drop_field` / `wrong_type` / `extra_field` / `target_out_of_range`
  corruption strategies (see `malformed.py` in both services).
- **Diagnosis steps**: consume the DLQ topic
  (`kafka-console-consumer.sh --topic higgs.features.dlq
  --from-beginning`) and inspect the `reason` field in each envelope —
  it names the Pydantic validation failure directly.
- **Recovery**: none needed automatically — routing to the DLQ *is* the
  handling. The message's Kafka offset is committed normally once it's
  in the DLQ, so it is never redelivered/reprocessed.
- **Data-loss implications**: the malformed message's raw bytes are
  preserved (base64-encoded in the DLQ envelope) for later inspection or
  reprocessing tooling — nothing is silently dropped.
- **Prevention/follow-up**: none needed for a simulator deliberately
  testing this path. In a real upstream-schema-drift incident, the next
  step would be a `schema.validation_failed` platform event (Milestone
  11+) and an on-call alert on DLQ volume — not yet built.

## higgs_pipeline: bulk upsert exceeds Postgres's parameter limit

Hit during the Milestone 2 acceptance run at ~48k rows extracted in one
window.

- **Symptom**: `update_curated_tables_task` fails with
  `psycopg.OperationalError: sending query and params failed: number of
  parameters must be between 0 and 65535`.
- **Likely cause**: a single multi-row `INSERT ... VALUES (...), (...)`
  built from one row per Airflow task execution — parameter count scales
  as `rows × columns`, and Postgres hard-caps the total per query
  regardless of how much memory/time you're willing to spend.
- **Diagnosis steps**: check `extracted_features`/`extracted_labels` on
  the failing run's `control.pipeline_runs` row (once the *next* run
  succeeds and reports it) or the task log's `Task failed with
  exception` line — the parameter count in the error message divided by
  the table's column count gives the approximate row count that broke it.
- **Recovery**: none needed once fixed — `higgs_pipeline_tasks.py`
  chunks every bulk upsert at `UPSERT_BATCH_SIZE = 2000` rows
  (`_chunked()`), keeping every single INSERT's parameter count an order
  of magnitude under the limit regardless of backlog size. A stuck task
  instance just needs a normal retry/rerun after the fix ships.
- **Data-loss implications**: none — the failed transaction rolled back
  entirely (nothing partially committed), and the watermark never
  advanced past that window, so the retry safely reprocessed it in full.
- **Prevention/follow-up**: `UPSERT_BATCH_SIZE` is a constant, not
  computed from table width — if a much wider table starts using this
  same upsert pattern, recompute the safe batch size for it explicitly
  rather than assuming 2000 is universally safe.

## higgs_pipeline: watermark regressed under concurrent DAG run retries

Observed during the Milestone 2 acceptance run when several runs that
had failed on the bugs above were all retried together after a container
rebuild, briefly overlapping in execution.

- **Symptom**: `control.pipeline_watermarks.watermark` moved *backward*
  — observed sitting at an earlier run's `upper_bound` (`20:10:00`) even
  though later runs (`upper_bound` `20:13:19`, `20:15:00`) had already
  completed successfully and should have advanced it further.
- **Likely cause**: `advance_watermark`'s original `ON CONFLICT DO
  UPDATE SET watermark = <new value>` overwrites unconditionally —
  whichever transaction *commits last* wins, which is not necessarily
  the run with the logically latest `upper_bound` when runs execute
  concurrently or out of order.
- **Diagnosis steps**: compare `control.pipeline_watermarks.watermark`
  against `MAX(features_watermark_after)` /
  `MAX(labels_watermark_after)` across `control.pipeline_runs` for
  successful runs — if the stored watermark is behind the max, this bug
  (or a variant of it) is present.
- **Recovery**: fixed by making the upsert monotonic — `SET watermark =
  GREATEST(current, new)` (`amel_lake/watermark.py`). No manual recovery
  was needed even before the fix landed: the next run simply
  re-extracted a wider (but safely deduplicated) window. Verified
  directly: triggering a run after the fix advanced the watermark from
  the regressed `20:10:00` to `20:25:00`, and the resulting wide
  re-extraction/re-upsert produced zero duplicate rows in any curated
  table.
- **Data-loss implications**: none — this class of bug can only cause
  wasted reprocessing (wider re-extraction windows), never data loss,
  *because* the upserts it gates are already idempotent. That
  idempotency is precisely what made this bug low-severity instead of a
  correctness incident.
- **Prevention/follow-up**: none further needed — the same `GREATEST`
  pattern should be used for any future checkpoint/watermark value that
  can be written by more than one execution context.

## feast materialize OOM-killed (and can take the whole WSL2 VM with it)

Exercised 2026-09-19: `feast materialize-incremental` over the ~885k-row
`curated.higgs_features_flat` view was killed by the kernel OOM killer at
**12.1 GB RSS** (`dmesg`: `Out of memory: Killed process ... (feast)
anon-rss:12147616kB`). The first Milestone 3 attempt, on a machine with
~18 unrelated containers also running, froze WSL2 outright at the same
step.

- Symptom: the `feast-materialize` container prints `Killed` and exits
  (exit code 0 through `docker compose run`, misleadingly — check
  `docker events --filter event=oom` or `dmesg | grep -i oom`); Redis
  `DBSIZE` is 0 or far below the curated row count; in the worst case
  WSL2 itself freezes and Docker Desktop has to be restarted.
- Likely causes: (1) an unbounded materialization window — Feast's
  Postgres offline store loads every row in the window into memory and
  converts each to protobufs, ~14 KB/row; (2) other memory-hungry
  containers leaving no headroom.
- Diagnosis steps:
  ```bash
  dmesg | grep -i 'out of memory'            # was it the global OOM killer?
  docker events --since 30m --filter event=oom --format '{{.Actor.Attributes.name}}'
  docker exec amel-redis-1 redis-cli DBSIZE  # vs:
  docker exec amel-postgres-1 psql -U amel -d amel -tAc "SELECT count(*) FROM curated.higgs_features"
  free -g; docker ps --format '{{.Names}}' | wc -l   # what else is eating memory?
  ```
- Recovery: assume Redis holds a partial materialization and wipe it —
  it is entirely derived state. Then re-run the *chunked* backfill:
  ```bash
  docker exec amel-redis-1 redis-cli FLUSHALL
  make feast-materialize      # scripts/materialize.py: ≤25k rows per window, resumes from the registry
  docker exec amel-redis-1 redis-cli DBSIZE   # must equal count(*) of curated.higgs_features
  ```
  If WSL2 froze: restart Docker Desktop, `make down`, `docker volume rm
  amel_redis-data`, `make up`, then the steps above.
- Data-loss implications: none — the online store is rebuilt from
  Postgres; nothing upstream is touched.
- Prevention/follow-up: never call `feast materialize` /
  `materialize-incremental` directly over this table; every Feast
  container now has a `mem_limit` (1–2 GB) so the blast radius is the
  container, not the VM; `scripts/materialize.py` and the Airflow task
  both chunk (measured peak 875 MiB for the full backfill). Stop
  unrelated containers before heavy runs — `docker ps` should show only
  `amel-*`.

## Feast offline queries fail with "server does not support SSL"

- Symptom: any historical retrieval or materialization fails with
  `psycopg ... server does not support SSL, but SSL was required`.
- Likely cause: Feast's Postgres offline store defaults `sslmode` to
  `require`; the local `postgres:16-alpine` has no SSL configured.
- Recovery: `sslmode: disable` under `offline_store:` in
  `ml/feature_repo/feature_store.yaml` (already set); re-run
  `feast-apply`.
- Prevention: keep the comment in `feature_store.yaml`; a production
  Postgres with TLS should flip this back to `require`.

## source_simulator producer wedge (Kafka stops accepting writes; /health lied)

Incident 2026-09-19: during a broker stall (Kafka starved by the OOM
above, plus a 2000 events/s burst) librdkafka timed out every in-flight
message (`_MSG_TIMED_OUT`), the publishing loop thread died on an
uncaught exception, and **`/health` kept returning 200 for over an
hour** while `rows_read` sat frozen at 184,052. Now reproduced on demand
by `make failure-simulator-wedge`
(`scripts/failure_engineering/simulator_producer_wedge.py`).

- Symptom: `curl localhost:8001/status` shows `counters.rows_read` not
  advancing; container CPU ~0%; `kafka_delivery_failed` errors in the
  simulator log; landing tables stop growing. After the fix: `/ready`
  is 503 with `kafka delivery failing: ...`, `/health` is 503 only if
  the loop actually died or librdkafka declared a fatal error.
- Likely causes: broker paused/overloaded/restarting; local producer
  queue full (`BufferError`) after a failed flush; idempotent-producer
  fatal error after a sequence gap.
- Diagnosis steps:
  ```bash
  curl -s localhost:8001/ready; echo; curl -s localhost:8001/health; echo
  curl -s localhost:8001/status | python3 -m json.tool | sed -n '/producer_health/,/}/p'
  docker logs --since 5m amel-source-simulator-1 | grep -c kafka_delivery_failed
  docker inspect -f '{{.State.Health.Status}}' amel-kafka-1
  ```
- Recovery: fix Kafka first. If `/health` is 200 the simulator recovers
  on its own once deliveries succeed (`/ready` flips back to 200 —
  verified: 0 s after `docker compose unpause kafka`). If `/health` is
  503, restart it: `docker compose -f infra/docker-compose.yml restart
  source-simulator` — and set `HIGGS_START_INDEX` first (next entry).
- Data-loss implications: events that timed out were never acked and
  are counted in `events_dropped`; the simulator is a *source*, so
  "loss" here just means fewer synthetic rows — nothing downstream is
  corrupted.
- Prevention/follow-up: delivery reports now feed the probes
  (`SimulatorRunner.record_delivery`); the loop catches publish errors
  and keeps going (dropping + counting) unless fatal;
  `KAFKA_MESSAGE_TIMEOUT_MS` defaults to 30 s (librdkafka's 300 s hid
  the outage for five minutes). Milestone 8's liveness/readiness probes
  point at `/health` and `/ready` — this entry is why they can be
  trusted. `make failure-simulator-wedge` is the regression test
  (measured: `/ready` → 503 after 32 s, 294 delivery failures recorded,
  self-recovery on unpause, `/health` correctly stayed 200).

## stream_ingestor evicted from its consumer group (commit rejected; /health lied)

Incident 2026-09-19, same root event as above: Kafka was starved long
enough that the broker dropped the ingestor from the group; the next
offset commit raised `KafkaException: Commit failed: Broker: Unknown
member`, which was uncaught, killed the consume loop thread, and left
`/health` at 200 with **~870k messages of lag and zero consumers in the
group** for an hour.

- Symptom: landing tables stop growing while the simulator's counters
  advance; `kafka-consumer-groups --describe --group stream-ingestor`
  shows `CONSUMER-ID -` (no members) and growing `LAG`;
  `stream_ingestor_stopped` followed by a `KafkaException` traceback in
  the container log. After the fix: `/health` 503 (`consume loop died:
  ...`) if the loop is dead, `/ready` 503 (`no partition assignment` /
  `last offset commit rejected: ...`) while degraded.
- Diagnosis steps:
  ```bash
  curl -s localhost:8002/status; echo
  docker run --rm --network amel_default confluentinc/cp-kafka:latest \
    kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group stream-ingestor
  docker logs amel-stream-ingestor-1 2>&1 | grep -v /health | tail -20
  ```
- Recovery: a rejected commit no longer kills the loop — the next poll
  rejoins the group and Kafka redelivers from the last committed offset;
  the idempotent sink absorbs the redelivery (`duplicate_events_skipped`
  in the log is expected). If `/health` is 503, `docker compose -f
  infra/docker-compose.yml restart stream-ingestor`; it drains the
  backlog (measured ~870k messages, mostly duplicates, in ~3 minutes).
- Data-loss implications: none — offsets were never committed for the
  affected batch, so nothing was skipped; rows written before the
  rejected commit are simply re-upserted as no-ops.
- Prevention/follow-up: `KafkaException` on commit is caught and
  surfaces through `/ready`; any other loop death is recorded and fails
  `/health`; `on_revoke`/`on_lost` clear readiness. Unit tests in
  `services/stream_ingestor/tests/test_ingestor_probes.py`.

## source_simulator restarted: every event is a duplicate, no new curated data

- Symptom: after any simulator restart, `duplicate_events_skipped`
  fills the ingestor log, landing/curated row counts stay flat, DAG runs
  succeed with 0 rows, `update_feature_store` reports
  `nothing_to_materialize`.
- Likely cause: `entity_id` is `higgs-{index}` from a counter that
  restarts at 0 on every boot; with ~885k ids already landed, the
  simulator replays ~2.5 hours of already-seen ids at 100 events/s
  before producing anything new.
- Recovery: restart it with the counter set past what has landed:
  ```bash
  MAXID=$(docker exec amel-postgres-1 psql -U amel -d amel -tAc "SELECT max(entity_id) FROM landing.higgs_feature_events")
  HIGGS_START_INDEX=$(( 10#${MAXID#higgs-} + 1 )) docker compose -f infra/docker-compose.yml up -d --no-deps source-simulator
  ```
- Prevention/follow-up: `HIGGS_START_INDEX` (config + `.env.example`);
  a future improvement is to have the simulator persist its own
  position, which Milestone 8's stateful deployment would need anyway.

## A new database is missing after adding a Postgres init script

- Symptom: a service fails with `database "mlflow" does not exist` (or
  `feast`, `airflow`) even though `infra/postgres/init/0N-create-*.sql`
  exists.
- Likely cause: `docker-entrypoint-initdb.d` scripts run **only when the
  data volume is first initialized**. An existing `amel_postgres-data`
  volume never sees a script added later.
- Recovery: create it by hand once —
  `docker exec amel-postgres-1 psql -U amel -d amel -c "CREATE DATABASE mlflow"`
  — or, if the volume is disposable, `make down && docker volume rm
  amel_postgres-data && make up` (this discards every landing/curated
  row; the pipeline will rebuild them from the simulator over time).
- Prevention: keep adding the init script (fresh checkouts need it) and
  note the manual step in the milestone's PROJECT_STATE entry.

## MLflow server: 403 "Invalid Host header", or sitting at its memory cap

- Symptom (a): every client call fails with `403 ... Invalid Host header
  - possible DNS rebinding attack detected`.
  Cause: MLflow 3 validates `Host`; `mlflow:5000` (in-network) and
  `localhost:5000` (from the host) must be in `--allowed-hosts`. That
  flag only works with the default uvicorn server — do not combine it
  with `--gunicorn-opts` (the server exits with a usage error).
- Symptom (b): `docker stats` shows `amel-mlflow-1` at ~1 GiB / 1 GiB
  right after boot.
  Cause: default 4 workers + the jobs subsystem (job runner + 2 huey
  consumers, ~200 MB each). Fix in place: `--workers=2` and
  `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false` (measured 483–534 MiB).
- Diagnosis: `docker logs amel-mlflow-1 | tail`, `curl -s
  localhost:5000/health`, `docker stats --no-stream amel-mlflow-1`.

## Promotion rejected (exit 2) — what now

- Symptom: `make promote` prints `REJECTED` with a `reasons` list and
  exits 2; `make model-show` still shows the previous champion.
- This is the system working: the candidate failed a floor
  (`TRAINING_PROMOTION_MIN_TEST_ACCURACY` / `_MIN_TEST_ROC_AUC`) or
  regressed the champion by more than `TRAINING_PROMOTION_MAX_REGRESSION`.
- Options: train a better candidate; adjust the criteria via env
  (a config change, visible in the next audit row's `criteria`); or
  `make promote PROMOTE_ARGS=--force` — recorded in
  `ml.model_promotions.reason` as `FORCED by operator`. Never edit the
  alias in the MLflow UI: that bypasses the audit row.
- Verify: `SELECT * FROM ml.model_promotions ORDER BY id DESC LIMIT 3;`

## inference_api serves a stale champion after a promotion

- Symptom: `make model-show` says champion = N but `curl
  localhost:8003/model` shows an older version; predictions carry the
  old `model.version`.
- Cause: by design. Promotion changes the registry; serving changes on
  refresh (`INFERENCE_MODEL_REFRESH_SECONDS=0` means manual only).
- Recovery: `curl -X POST localhost:8003/model/refresh -H
  "X-Admin-Token: $INFERENCE_ADMIN_TOKEN"` → expect `"swapped": true`.
  If `"error"` is set, MLflow is unreachable: the previous model keeps
  serving; fix MLflow, refresh again (the error clears on the next
  successful call, even a no-op).
- Rollback: `make promote PROMOTE_ARGS="--version <old> --force"` (or
  re-alias in MLflow with a matching audit row), then refresh.

## inference_api /ready is 503

- Read the body — it names the dependency: `no model loaded: ...`
  (initial load failed; check MLflow and refresh), `feast-server
  unreachable`, `database unreachable: ...`, `kafka delivery failing:
  ...`. `/health` stays 200 in all of these; only the process being
  wedged should fail liveness.
- `curl localhost:8003/metrics | grep amel_inference_` for
  `persist_failures_total` / `publish_failures_total` trends.
