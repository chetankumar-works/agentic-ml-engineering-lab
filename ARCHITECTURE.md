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

## Data flow, current (Milestone 0)

No runtime data flow yet. This project has tooling and documentation
only. First data flow (source simulator → Kafka → consumer → Postgres)
lands in Milestone 1.

## Reliability properties established so far

None yet at the systems level — Milestone 0 is tooling/environment only.
`DECISIONS.md` ADR-0001 documents the one reliability-adjacent decision
made so far: pinning the Python interpreter so dependency installs are
reproducible across sessions and machines.

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
