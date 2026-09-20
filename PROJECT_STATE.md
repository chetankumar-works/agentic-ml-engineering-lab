# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 6 — OpenTelemetry and observability stack.** Complete.

## Completed work (Milestone 6; earlier milestones in MILESTONE_REPORT.md)

- **`amel_common.telemetry`**: `configure_telemetry()` (tracer + logger
  providers, OTLP/HTTP export, resource attrs), `instrument_fastapi/
  sqlalchemy/httpx/kafka_producer/kafka_consumer`, `current_trace_ids()`.
  Opt-in via `OTEL_ENABLED` (Compose sets it; tests never need a
  collector).
- **`amel_common.logging`** now routes structlog through stdlib so the
  OTLP `LoggingHandler` ships every line with `trace_id`/`span_id`.
- **Instrumented**: `inference_api` (FastAPI, httpx→feast-server,
  SQLAlchemy, Kafka producer; `X-Trace-Id` = OTel trace id),
  `source_simulator` (FastAPI, Kafka producer), `stream_ingestor`
  (FastAPI, Kafka consumer + `ingest_batch` spans, SQLAlchemy),
  `amel_db` engine, **feast-server** (`opentelemetry-instrument`),
  **Airflow** (native OTel, gRPC).
- **Infra** (`infra/observability/`): OTel Collector 0.161 (traces →
  Tempo, logs → Loki OTLP, `spanmetrics` → Prometheus exporter), Tempo
  3.0, Loki 3.7, Prometheus 3.14 (+ kafka-exporter 1.10 for consumer
  lag), Grafana 13.2 provisioned with Prometheus/Tempo/Loki/AMEL
  Postgres datasources, trace↔logs links, and the "AMEL overview"
  dashboard (11 panels). All capped; 24 h / 2 d retention.
- **`scripts/smoke_milestone6.py`** / `make smoke-tracing`.
- `DECISIONS.md` ADR-0008; docs updated. 89 unit tests (unchanged —
  telemetry is off under test).

## Milestone 6 acceptance run

Run 2026-09-20 against the live stack (`make smoke-tracing`, plus
manual checks).

```
=== 1. prediction ===
  entity higgs-001600326 -> prediction 1 (model v5)
  trace 90eb3fe29dfa1467e2262db765be6e6f          (== X-Trace-Id response header)
=== 2. Tempo: the trace spans two services ===
  13 spans across ['feast_server', 'inference_api']
     inference_api SERVER  POST /predict/entity/{entity_id}
     inference_api CLIENT  POST                       -> feast_server SERVER POST /get-online-features
     feast_server  CLIENT  HMGET (Redis)
     inference_api CLIENT  INSERT amel (ml.predictions)
     inference_api PRODUCER predictions.v1 send
=== 3. Loki: a log line carries the trace id ===   1 line ({"event": "prediction_request", "trace_id": ...})
=== 4. Postgres: the ml.predictions row carries it ===   1 row
=== 5. Prometheus ===  6 targets up; RED (spanmetrics) for airflow, feast_server, inference_api,
                       source_simulator, stream_ingestor; kafka_consumergroup_lag{stream-ingestor} present
Milestone 6 smoke test PASSED

Ingestion: simulator `higgs.features.v1 send` producer spans; ingestor `recv` spans linked to them
           + `ingest_batch` spans (OTel batch-consumer semantics, ADR-0008).
Airflow:   `dag_run.higgs_pipeline` root traces (19.3 s and 444.5 s) with task spans, via gRPC.
Grafana:   all 4 datasources healthy; dashboard loads; Postgres panel returns ml.predictions count.
Memory:    collector 129 MiB, Tempo 279, Loki 124, Prometheus 139, Grafana 191, kafka-exporter 30
           (≈ 0.9 GB); all containers ≈ 9.0 GB of 15.
```

**Milestone 6 acceptance verified**: a single prediction is traced
across service boundaries (inference_api → feast_server → Redis, plus
Postgres and Kafka), with logs, metrics and the DB row correlated on one
id.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute")

Unchanged from Milestone 0–5 (re-verify at the start of Milestone 7) —
see git history for the full table. Additions this milestone:
`opentelemetry-sdk 1.44.0` / instrumentations `0.65b0`; images
`otel/opentelemetry-collector-contrib:0.161.0`, `grafana/tempo` 3.0.0,
`grafana/loki` 3.7.8, `prom/prometheus` 3.14.0, `grafana/grafana` 13.2.2,
`danielqsj/kafka-exporter` 1.10.0 (the `latest` tags resolved to these on
2026-09-20 — pin in Milestone 7).

## Current known failures / gaps

- Only first-party Python logs reach Loki; Kafka/Postgres/MinIO/Airflow
  container logs are `docker logs` only (Alloy/Promtail is the upgrade,
  planned with Kubernetes in Milestone 8).
- Kafka consumer spans are linked, not parented (by design); an
  end-to-end "one trace per event" view needs a different pattern.
- No alert rules; Grafana is anonymous-admin (dev only); one dashboard.
- Airflow ignores `OTEL_EXPORTER_OTLP_PROTOCOL` (gRPC only).
- Redis keeps growing with the simulator (~200 MB/h of new entities at
  100 events/s; 0.8 GB at 1.6 M) — consider `key_ttl_seconds` on the
  Feast online store or pausing the simulator when idle.
- Carried: shared admin token; synchronous persist/publish; promotion
  on own test split; manual DB creation on existing volumes; simulator
  position manual; no CI (Milestone 7); no dedicated ingestor failure
  script.

## Commands that work today

```bash
make install       # uv sync --all-packages
make lint / fmt / typecheck / test   # 89 tests, no infra

make up             # everything incl. otel-collector, tempo, loki, prometheus, kafka-exporter, grafana
make logs / make down
make smoke · make feast-materialize · make feast-demo · make failure-simulator-wedge
make train · make promote DECIDED_BY=you · make model-show
make smoke-tracing  # Milestone 6 acceptance

# UIs: Grafana http://localhost:3000 · Prometheus :9090 · Tempo :3200 · Loki :3100
#      MLflow :5000 · Airflow :8080 · inference API :8003/docs
curl -s "localhost:3200/api/traces/<trace_id>"
curl -s -G localhost:3100/loki/api/v1/query_range --data-urlencode 'query={service_name="inference_api"} |= "<trace_id>"'
curl -s -G localhost:9090/api/v1/query --data-urlencode 'query=sum by (service_name) (traces_span_metrics_calls_total)'
```

## Next task

**Milestone 7 — Docker hardening + CI/CD.**

Acceptance: a clean checkout can start the local environment using
documented commands, and GitHub Actions passes on the pushed branch.
Concretely: (1) prove the clean-checkout path — fresh clone, `make
install`, `make up` on an empty volume set (the init scripts must create
`airflow`/`feast`/`mlflow` DBs; the simulator must fetch the dataset or
be pointed at a cached copy), `make smoke`, `make feast-materialize`,
`make train`; (2) Docker hardening: non-root users where missing
(feast/training/mlflow images run as root), pinned base images and
image tags (`latest` → versions verified this milestone), `.dockerignore`,
healthchecks on every long-running service, restart policies, resource
caps everywhere; (3) GitHub Actions: lint + typecheck + unit tests on
push/PR, image builds, and a lightweight integration job if feasible
within runner limits; (4) update MILESTONE_REPORT/ARCHITECTURE. The
kind/minikube memory plan is due *before* Milestone 8.
