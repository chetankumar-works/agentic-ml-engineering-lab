# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 5 — FastAPI inference.** Complete.

## Completed work (Milestone 5; earlier milestones in MILESTONE_REPORT.md)

- **`apps/inference_api`** (`inference-api`, uv workspace member —
  `apps/*` added to the workspace; every workspace Dockerfile now
  copies `apps/`): FastAPI on port 8003 with `GET /health`, `GET /ready`,
  `GET /model`, `POST /model/refresh` (`X-Admin-Token`),
  `POST /predict/raw`, `POST /predict/entity/{entity_id}`,
  `GET /metrics`; `/docs` OpenAPI. Modules: `model.py` (`ModelCache`,
  atomic swap, failure keeps serving), `features.py` (feast-server over
  HTTP), `store.py` (`ml.predictions`), `publisher.py`
  (`predictions.v1`), `service.py`, `api.py`. 9 unit tests with fakes.
- **`PredictionEvent`** schema in `amel_common.schemas` (v1.0.0).
- **Alembic `0005`** `ml.predictions` (+ `Prediction` model), indexed by
  entity, model version, created_at.
- **Compose `inference-api`** service (depends on mlflow, feast-server,
  kafka-init, migrate; `mem_limit: 1g`; measured ~246 MiB; MLflow client
  retries capped so refresh fails fast).
- `DECISIONS.md` ADR-0007; docs updated. 80 → **89 unit tests**.

## Milestone 5 acceptance run

Run 2026-09-19 against the live stack.

```
Start-up: model_swapped previous=null current=3 (models:/higgs_decision_tree@champion)
GET /model -> version 3, run b6cda9fb..., git_sha a70d233..., dataset_fingerprint ff72b440727cb949,
             feature_view higgs_features:v1, refresh_policy "manual: POST /model/refresh"
GET /health 200 ok · GET /ready 200 ready

POST /predict/entity/higgs-001368269 (X-Trace-Id: trace-demo-entity)
  -> prediction 1, probability 0.7061068702290076, source entity, persisted true, 87 ms (cold), model v3
POST /predict/raw with the same entity's features read from curated.higgs_features_flat
  -> prediction 1, probability 0.7061068702290076   (online == offline, to the last digit)
POST /predict/entity/higgs-999999999 -> 404 "no online features for entity ..."
POST /predict/raw {"lepton_pt": 1.0} -> 422 naming the 27 missing fields
200 sequential /predict/entity calls: p50 5.7 ms, p95 6.8 ms, max 7.5 ms (feast fetch + DB write + Kafka)
ml.predictions: 203 rows; predictions.v1: events with prediction_id/model_version/trace_id matching the responses
/metrics: amel_inference_predictions_total{source="entity",outcome="ok"} 201, persist_failures 0, publish_failures 0

Controlled refresh:
  trained v5 (as_of 21:30, test_acc 0.6956) -> make promote -> PROMOTED (audit row 2, prev champion 3)
  served BEFORE refresh: v3 (promotion alone does not change serving)
  POST /model/refresh without token -> 401
  POST /model/refresh with token -> {"previous_version":"3","current_version":"5","swapped":true}
  next prediction: model v5, git_sha 0c84c02

MLflow outage: docker compose stop mlflow ->
  POST /predict/entity -> 200 · GET /ready -> 200 (registry is not a serving dependency)
  POST /model/refresh -> error after 20 s (was ~3 min with default client retries), still serving v5
  after start mlflow -> refresh -> {"swapped":false,"error":null}
Memory: inference-api 246 MiB / 1 GiB
```

**Milestone 5 acceptance verified**: the API loads the champion model
and returns predictions with model metadata (version, run id, git SHA,
dataset fingerprint, feature view), over both raw and Feast-online
features, with prediction id + trace id, persisted and published.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute")

Unchanged from Milestone 0–4 (re-verify at the start of Milestone 6) —
see git history for the full table. Additions this milestone:
`fastapi==0.141.1`, `httpx` (feast-server client). MLflow client retries capped via env for the API.

## Current known failures / gaps

- `POST /model/refresh` is guarded by a shared token, not JWT scopes
  (Milestone 11 platform API / auth).
- Persistence and publishing are synchronous in the request path
  (fine at 5.7 ms p50; revisit under load in Milestone 10).
- No batch prediction endpoint; no canary/shadow serving of `@candidate`.
- `ml.model_metadata` (suggested in the spec) not created — MLflow's
  registry + `ml.model_promotions` cover it; revisit if the platform
  API needs a local mirror.
- Carried: promotion compares each run's own test split; 200k training
  default; `train` rebuilds per invocation; manual DB creation on
  existing volumes; simulator position manual on restart; synchronous
  per-run materialization; plain VIEW; download hardening; no CI
  (Milestone 7); no dedicated ingestor failure script.

## Commands that work today

```bash
make install       # uv sync --all-packages
make lint / fmt / typecheck / test   # all pass, no infra required (89 tests)

make up             # + inference-api (port 8003)
make logs / make down
make smoke · make feast-materialize · make feast-demo · make failure-simulator-wedge
make train · make promote DECIDED_BY=you · make model-show

# Inference API (Milestone 5):
curl -s localhost:8003/model | python3 -m json.tool
curl -s -X POST localhost:8003/predict/entity/<entity_id> -H 'X-Trace-Id: demo'
curl -s -X POST localhost:8003/predict/raw -H 'Content-Type: application/json' \
  -d '{"features": {<28 HIGGS names>: <float>}, "entity_id": "optional"}'
curl -s -X POST localhost:8003/model/refresh -H 'X-Admin-Token: dev-admin-token'
curl -s localhost:8003/ready; curl -s localhost:8003/metrics | grep amel_inference_
docker exec amel-postgres-1 psql -U amel -d amel -c "SELECT * FROM ml.predictions ORDER BY created_at DESC LIMIT 5"
docker run --rm --network amel_default confluentinc/cp-kafka:latest kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic predictions.v1 --from-beginning --timeout-ms 5000
# OpenAPI: http://localhost:8003/docs
```

## Next task

**Milestone 6 — OpenTelemetry and observability stack.**

Acceptance: a single prediction can be traced across relevant service
boundaries. Per AMEL_KICKOFF_PROMPT.md "Observability": OpenTelemetry
SDK in every first-party service (inference_api → feast-server call,
Postgres write, Kafka publish; stream_ingestor consume → DB; simulator
publish), an OTel Collector, Prometheus (scrape `/metrics`), Tempo
(traces), Loki (logs), Grafana (dashboards: ingestion rate, DLQ rate,
pipeline runs, prediction latency, model version). Trace id already
propagates via `X-Trace-Id` and Kafka message headers are the next hop.
Memory plan first: Prometheus + Tempo + Loki + Grafana + Collector is
five more containers — budget ~1.5 GB total with tight caps and short
retention; measure before wiring dashboards.
