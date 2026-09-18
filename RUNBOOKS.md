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
