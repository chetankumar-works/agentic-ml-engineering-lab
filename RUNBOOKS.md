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
