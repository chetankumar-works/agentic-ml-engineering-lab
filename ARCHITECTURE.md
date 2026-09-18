# Architecture

This document describes the target system design. It is written ahead of
full implementation (per the build order in `PROJECT_STATE.md`), so
sections describing unbuilt components are marked **(planned)**. Update a
section's status to built and link the relevant code/docs as each
milestone lands — don't let this drift into aspirational fiction.

## Why this shape

AMEL simulates a realistic ML platform's operational surface: events
arrive continuously, must be validated and persisted exactly once, get
batch-processed into curated training data on a schedule, feed a feature
store that serves both training and low-latency inference, produce models
that are tracked and promoted deliberately (not silently), and the whole
thing is observable, deployable, scalable, and eventually operable by a
constrained agent system rather than only by a human. Each subsystem below
exists because a real ML platform needs it — not because it demonstrates a
buzzword.

## Layered build order **(architecture intent, tracked live in PROJECT_STATE.md)**

1. **Data plane**: source simulator → Kafka → stream consumer → Postgres
   (`landing` schema). At-least-once delivery + idempotent sink. See
   ADR references in `DECISIONS.md` as ingestion is built.
2. **Batch plane**: Airflow DAG promotes `landing` → `curated` via MinIO
   bronze/silver/gold Parquet, driven by watermarks, not full rescans.
3. **Feature plane**: Feast reads `curated`/Parquet offline, materializes
   to Redis online. Training reads offline (point-in-time correct);
   inference reads online (latest).
4. **Model plane**: reproducible training → MLflow tracking + registry,
   with explicit `candidate`→`champion` promotion criteria before
   `apps/inference_api` will serve a model.
5. **Serving plane**: `apps/inference_api` (predictions),
   `apps/platform_api` (control plane), `apps/webhook_service`
   (external event ingestion).
6. **Observability plane**: OpenTelemetry across all first-party
   services → Collector → Prometheus/Tempo/Loki/Grafana.
7. **Orchestration/scale plane**: Docker Compose → Kubernetes → HPA/KEDA.
8. **Automation plane**: normalized platform events + metadata graph →
   Agent Supervisor + specialized agents (read-only tools first) → RAG
   over runbooks → Temporal for durable, approvable remediation
   workflows → MCP as a thin read-only interface over the same
   service-layer functions the agents already call.
9. **Access plane**: Python SDK → CLI → minimal web app, all three
   against the same REST contracts.
10. **FinOps + serverless**: cost accounting as a cross-cutting concern
    once there's something (LLM calls, compute) to account for.

Each layer is built only after the one below it demonstrably works — see
`PROJECT_STATE.md`'s milestone list for the authoritative sequencing and
current status.

## Data flow, current (Milestone 2)

```
UCI HIGGS zip (downloaded once, streamed row-by-row, never fully
materialized) → source_simulator
    → higgs.features.v1 (immediate)     → stream_ingestor consumer group
    → higgs.labels.v1 (delayed by LABEL_DELAY_SECONDS)  ↗
         (deliberate duplicates + malformed payloads injected per
          DUPLICATE_RATE / INVALID_EVENT_RATE)

stream_ingestor: batch → validate (shared Pydantic schema) →
    valid    → landing.higgs_feature_events / landing.higgs_label_events
               (INSERT ... ON CONFLICT (event_id) DO NOTHING, one
               transaction per batch, Kafka offsets committed only after
               that transaction commits)
    invalid  → higgs.features.dlq / higgs.labels.dlq (envelope with
               reason + raw bytes; offset committed immediately — see
               LEARNING_LOG.md)

higgs_pipeline (Airflow DAG, higgs_pipeline_tasks.py has the logic):
  determine_high_watermark  (control.pipeline_watermarks: higgs_features / higgs_labels)
    → extract_new_records   (landing.* WHERE ingested_at in (watermark, upper_bound])
    → write_bronze          (MinIO bronze/higgs/{features,labels}/dt=.../run_id=....parquet — raw shape)
    → validate               (Pandera; invalid rows quarantined, report → MinIO pipeline/artifacts/...)
    → transform               (flatten nested `features` JSON → 28 typed float columns)
    → write_silver            (MinIO silver/... — analytics-ready shape)
    → update_curated_tables   (upsert curated.higgs_features/higgs_labels
                                + advance both watermarks — ONE transaction)
    → build_training_dataset  (join ACCUMULATED curated.higgs_features/higgs_labels
                                for entities missing from curated.training_records —
                                not this run's delta, so late-arriving labels are
                                still picked up; upsert + MinIO gold/training/....parquet)
    → update_feature_store    (stub — Feast lands in Milestone 3)
    → emit_pipeline_metadata  (one control.pipeline_runs row per run;
                                on_failure_callback covers the failure path too)
```

Implemented in `libs/amel_common` (shared schemas/logging), `libs/amel_db`
(models + Alembic migrations), `libs/amel_lake` (MinIO client, object
keys, Pandera validation, watermark read/advance),
`services/source_simulator`, `services/stream_ingestor`,
`infra/airflow/dags/`. Full reasoning, the three bugs found during the
acceptance run, and what was actually verified: `LEARNING_LOG.md`'s
Milestone 1 and Milestone 2 entries and `RUNBOOKS.md`.

## Reliability properties established so far

- **At-least-once delivery + idempotent sink** (Milestone 1): a crash
  between the DB write and the Kafka offset commit causes a harmless
  redelivery, never silent data loss and never a duplicate row. Verified
  with a real `SIGKILL` mid-processing — see `RUNBOOKS.md`.
- **Dead-letter handling as a deliberate path**, not an afterthought: a
  schema-invalid message is routed and its offset committed immediately,
  so it can never poison-pill a partition by blocking redelivery forever.
- **Watermark-gated, idempotent batch curation** (Milestone 2): the
  watermark only advances in the same transaction as the curated upsert
  it gates, and the upsert itself is `ON CONFLICT DO NOTHING` — so even
  when the watermark's own monotonicity briefly broke under concurrent
  retries (a real bug, see LEARNING_LOG.md), the *data* stayed correct;
  only the (safely deduplicated) re-extraction window widened.
- **Data-quality gate as a first-class pipeline stage**: `validate`
  quarantines bad rows and fails the run above a configurable invalid
  threshold, rather than either silently dropping bad data or letting it
  flow downstream unchecked.
- Pinning the Python interpreter (`DECISIONS.md` ADR-0001) so dependency
  installs are reproducible across sessions and machines.

## Cross-cutting concerns **(planned, referenced here so later docs can link back)**

- **Idempotency**: `event_id` uniqueness enforced at the Postgres sink
  (Milestone 1); consumer offset commits happen only after a successful,
  idempotent write.
- **Schema evolution**: all inter-service payloads are versioned Pydantic
  schemas (`schema_version` field), never raw dicts.
- **Authorization**: JWT-based roles/scopes (Milestone 5+ for the APIs
  that need it), agent state-changing actions additionally gated by risk
  classification + policy evaluation (Milestone 12+).
- **Observability**: trace IDs propagate from ingestion through
  inference; every first-party service emits structured logs correlated
  to trace IDs.
