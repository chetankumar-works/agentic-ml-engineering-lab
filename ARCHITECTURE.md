# Architecture

This document describes the target system design. It is written ahead of
full implementation (per the build order in `PROJECT_STATE.md`), so
sections describing unbuilt components are marked **(planned)**. Update a
section's status to built and link the relevant code/docs as each
milestone lands — don't let this drift into aspirational fiction.

## Start here (no prior ML/platform-engineering experience required)

**What AMEL actually does, in one paragraph.** A stream of physics
detector readings (the HIGGS dataset, replayed as if it were live) flows
continuously into the system. AMEL's job is to take that raw, messy,
continuously-arriving stream and turn it into: (1) a clean historical
record nothing loses or duplicates, (2) a trustworthy, validated dataset
other systems can build on, (3) a trained model that predicts something
useful from it, (4) an API that serves live predictions from that model,
and (5) enough visibility into all of the above that a human — or later,
a constrained AI agent — can tell what's happening and fix it when it
breaks. Every numbered layer below is one of those five things, built in
order, each one proven working before the next is added.

**Why build it this way instead of just training a model on a CSV?**
Because "train a model" is the easy 5% of real ML engineering. The other
95% — the part this project is actually about — is everything a model
needs *around* it to be useful and trustworthy in production: data has to
arrive reliably even when things crash mid-stream, be validated before
anyone trusts it, be reproducible enough that "what changed?" has an
answer, be servable with predictable latency, and be observable enough
that "why did this break?" has an answer too. AMEL builds each of those
properties as a real, working, verified system — not a slide describing
one.

**The system end to end, as it will look once every layer lands:**

```mermaid
flowchart LR
    subgraph Source["1. Data enters the system"]
        SIM[source_simulator<br/>replays HIGGS as live events]
    end
    subgraph Stream["Kafka — a durable delivery pipe"]
        K1[higgs.features.v1]
        K2[higgs.labels.v1]
    end
    subgraph Ingest["2. Safe landing"]
        ING[stream_ingestor<br/>dedupe + persist]
        PG1[(Postgres<br/>landing schema)]
    end
    subgraph Batch["3. Batch curation"]
        AF[Airflow DAG<br/>bronze/silver/gold]
        MINIO[(MinIO object storage)]
        PG2[(Postgres<br/>curated schema)]
    end
    subgraph Features["4. Feature store (planned)"]
        FEAST[Feast]
        REDIS[(Redis<br/>online features)]
    end
    subgraph Model["5. Model lifecycle (planned)"]
        TRAIN[training job]
        MLF[(MLflow<br/>tracking + registry)]
    end
    subgraph Serve["6. Serving (planned)"]
        API[inference_api]
    end
    subgraph See["7. Observability (planned)"]
        OTEL[OpenTelemetry]
        GRAF[Grafana]
    end

    SIM --> K1 & K2 --> ING --> PG1
    PG1 --> AF --> MINIO
    AF --> PG2
    PG2 --> FEAST --> REDIS
    MINIO --> TRAIN --> MLF --> API
    REDIS --> API
    ING -.emits traces/logs.-> OTEL
    AF -.emits traces/logs.-> OTEL
    API -.emits traces/logs.-> OTEL
    OTEL --> GRAF
```

Boxes 1–3 are built and verified today (Milestones 1–2). Boxes 4–7 are
planned — see the milestone-by-milestone build order below and
`PROJECT_STATE.md` for exactly what's real right now versus what's
still ahead.

**A short glossary, since the rest of this document uses these terms
constantly without re-explaining them:**

| Term | Plain-language meaning |
|---|---|
| **Idempotent** | Doing the same operation twice has the same effect as doing it once. The whole system leans on this constantly: if a message gets delivered twice (which distributed systems do, routinely), an idempotent write just quietly no-ops the second time instead of creating a duplicate. |
| **At-least-once delivery** | A messaging guarantee that a message will arrive one *or more* times, never zero times. Kafka gives you this by default. Combined with idempotent writes, "one or more times" becomes as safe as "exactly once" in practice. |
| **Watermark** | A saved "I've processed everything up to here" bookmark. Each pipeline run reads the watermark, processes everything newer than it, then moves the watermark forward — so a rerun never reprocesses old data from scratch. |
| **DAG** | "Directed Acyclic Graph" — Airflow's term for a pipeline: a set of steps with dependencies between them (step B can't start until step A finishes), and no step depends on itself, directly or in a loop. |
| **Bronze / silver / gold** | A common data-lake naming convention for "how refined is this data": bronze = raw copy exactly as it arrived, silver = cleaned and typed, gold = ready for a specific consumer (here, model training). |
| **Feature store** | A system that serves the same engineered features to both model training (historical, point-in-time-correct) and live inference (latest values, low latency) — so the model never sees different logic in the two places, a common and hard-to-debug source of production ML bugs. |
| **Point-in-time correctness** | When building a training example for "what happened at time T," only using data that was actually known *before* T — not accidentally leaking a future value into a historical training row. |
| **Idempotent sink** | The specific technique used here: an `event_id` as a database primary key plus `INSERT ... ON CONFLICT DO NOTHING`, so redelivering the same event a second time is a safe no-op instead of a duplicate row. |
| **Consumer group / offset** | Kafka concepts: a *consumer group* is one or more processes cooperatively reading a topic; an *offset* is "how far into the topic has this group read." Committing an offset means "don't send me this message again." |
| **Schema-valid but still wrong** | A message can pass structural validation (right fields, right types) while still being *semantically* wrong (a sensor value 1000x too large, a duplicate ID within a batch). AMEL validates for both, at different layers — see the "validate" pipeline stage. |

If a term shows up elsewhere in this document that isn't in that table,
`LEARNING_LOG.md` explains it in more depth in whichever milestone's
entry introduced it — that file is written explicitly to teach, not just
to record.

## Diagrams (Mermaid — GitHub renders these; update them as the system grows)

Three views, all of what is **built** as of Milestone 3 unless a node is
marked *(planned)*. `PROJECT_STATE.md` is authoritative for status.

### System context

```mermaid
flowchart LR
    dev(["Developer / operator"]) -->|"make up, triggers DAGs,<br/>reads dashboards"| amel
    higgs[("UCI HIGGS dataset<br/>~2.8 GB zip, cached locally")] -->|"streamed row by row"| amel

    subgraph amel ["AMEL — Docker Compose on WSL2 (15 GB)"]
        direction TB
        sim["source_simulator"]
        ing["stream_ingestor"]
        af["Airflow: higgs_pipeline"]
        fs["Feast feature server"]
        inf["inference_api<br/>(planned, M5)"]
        plat["platform_api / agents<br/>(planned, M11+)"]
    end

    amel -->|"predictions"| clients(["API clients<br/>(planned)"])
    amel -->|"metrics, traces, logs"| obs["Prometheus / Grafana / Tempo / Loki<br/>(planned, M6)"]
```

### End-to-end data flow (built through Milestone 3)

```mermaid
flowchart TB
    higgs[("HIGGS zip")] --> sim

    subgraph ingest ["Milestone 1 — streaming ingestion"]
        sim["source_simulator<br/>entity_id = higgs-{HIGGS_START_INDEX + i}<br/>dup / malformed injection"] -->|"higgs.features.v1"| kafka[("Kafka, KRaft")]
        sim -->|"higgs.labels.v1<br/>delayed LABEL_DELAY_SECONDS"| kafka
        kafka --> ing["stream_ingestor<br/>validate → batch upsert<br/>commit offsets after DB commit"]
        ing -->|"ON CONFLICT DO NOTHING"| landing[("Postgres<br/>landing.higgs_feature_events<br/>landing.higgs_label_events")]
        ing -->|"invalid"| dlq[("higgs.*.dlq")]
    end

    subgraph batch ["Milestone 2 — batch curation"]
        landing -->|"ingested_at > watermark"| dag["Airflow higgs_pipeline<br/>every 5 min"]
        dag -->|"bronze: raw parquet"| minio[("MinIO<br/>bronze / silver / gold / pipeline")]
        dag -->|"silver: validated, flat"| minio
        dag -->|"gold: training parquet"| minio
        dag -->|"upsert + advance watermark<br/>(one transaction)"| curated[("Postgres<br/>curated.higgs_features<br/>curated.higgs_labels<br/>curated.training_records<br/>control.*")]
    end

    subgraph feature ["Milestone 3 — feature store"]
        curated -->|"VIEW curated.higgs_features_flat<br/>28 typed columns"| offline["Feast offline store<br/>(Postgres)"]
        registry[("Feast SQL registry<br/>Postgres db 'feast'")]
        offline -->|"chunked materialize<br/>≤25k rows/window"| redis[("Redis<br/>online store")]
        dag -->|"update_feature_store<br/>POST /materialize per window"| server["feast-server<br/>feast serve, 1 GB cap"]
        server --> redis
        server -.-> registry
    end

    subgraph consumers ["Consumers"]
        offline -->|"get_historical_features<br/>point-in-time join"| train["Training<br/>(planned, M4)"]
        redis -->|"get_online_features<br/>latest values"| infer["inference_api<br/>(planned, M5)"]
        curated -->|"entity df from labels"| train
    end
```

### What each milestone added

```mermaid
flowchart LR
    subgraph M0 ["M0 — skeleton"]
        m0a["uv workspace, Python 3.12 pin"]
        m0b["ruff / mypy / pytest / Makefile"]
        m0c["docs: ARCHITECTURE, DECISIONS,<br/>RUNBOOKS, LEARNING_LOG, PROJECT_STATE"]
    end
    subgraph M1 ["M1 — streaming ingestion"]
        m1a["libs/amel_common<br/>versioned Pydantic events"]
        m1b["libs/amel_db<br/>SQLAlchemy + Alembic 0001"]
        m1c["services/source_simulator"]
        m1d["services/stream_ingestor"]
        m1e["Postgres + Kafka (KRaft)<br/>+ 7 topics + DLQs"]
    end
    subgraph M2 ["M2 — batch pipeline"]
        m2a["libs/amel_lake<br/>MinIO client, keys, Pandera, watermark"]
        m2b["Airflow 3.3.2 standalone<br/>higgs_pipeline DAG"]
        m2c["MinIO bronze/silver/gold"]
        m2d["Alembic 0002<br/>curated.* + control.*"]
    end
    subgraph M3 ["M3 — feature store"]
        m3a["ml/feature_repo<br/>Feast entity + feature view v1"]
        m3b["Alembic 0003<br/>curated.higgs_features_flat VIEW"]
        m3c["Redis online store<br/>feast-server (always-on)"]
        m3d["chunked materialization<br/>+ real update_feature_store"]
        m3e["honest liveness/readiness<br/>in simulator + ingestor<br/>failure-engineering script"]
    end
    subgraph next ["M4+ (planned)"]
        m4["Training + MLflow"]
        m5["inference_api / platform_api"]
        m6["OpenTelemetry stack"]
        m8["Kubernetes"]
    end
    M0 --> M1 --> M2 --> M3 --> next
```


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

## Data flow, current (Milestone 3)

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
    → update_feature_store    (rows this run inserted, by source_run_id →
                                ≤25k-row windows → POST feast-server /materialize
                                per window → Redis; zero rows → no call)
    → emit_pipeline_metadata  (one control.pipeline_runs row per run;
                                on_failure_callback covers the failure path too)

Feast (ml/feature_repo, DECISIONS.md ADR-0005):
  registry      Postgres db `feast` (SQL registry; feature_view_version_history)
  offline       Postgres VIEW curated.higgs_features_flat (28 typed columns
                over curated.higgs_features' JSONB) — point-in-time joins for training
  online        Redis, one key per entity_id, latest values — inference lookups
  backfill      make feast-materialize (scripts/materialize.py, chunked, resumable)
  steady state  the DAG task above, every 5 minutes
  serving       feast-server: POST /get-online-features, POST /materialize
```

Implemented in `libs/amel_common` (shared schemas/logging), `libs/amel_db`
(models + Alembic migrations), `libs/amel_lake` (MinIO client, object
keys, Pandera validation, watermark read/advance),
`services/source_simulator`, `services/stream_ingestor`,
`infra/airflow/dags/`, `ml/feature_repo`. Full reasoning, the bugs found
during each acceptance run, and what was actually verified:
`LEARNING_LOG.md`'s Milestone 1–3 entries, `MILESTONE_REPORT.md`, and
`RUNBOOKS.md`.

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
- **Online/offline feature consistency by construction** (Milestone 3):
  one feature definition serves both paths; materialization is recorded
  per window in the registry; the acceptance demo asserts Redis values
  equal Postgres values and that as-of-earlier requests return nothing
  (no future leakage).
- **Bounded memory for batch materialization** (Milestone 3): every
  materialization is chunked (≤25k rows) and every Feast container is
  memory-capped, after an unbounded run reached 12 GB and was OOM-killed.
- **Honest probes** (Milestone 3): `source_simulator` and
  `stream_ingestor` derive `/health` (liveness) and `/ready` (readiness)
  from real producer/consumer state — a dead loop or fatal producer fails
  liveness, current delivery/commit errors fail readiness — after both
  services were found "healthy" while functionally dead for an hour.
  `make failure-simulator-wedge` reproduces the incident as a test.
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
