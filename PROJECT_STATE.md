# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 4 — Training package + MLflow.** Complete.

## Completed work (Milestone 4; earlier milestones in MILESTONE_REPORT.md)

- **`ml/training`** (`amel-training`, uv workspace member): `config.py`
  (`TRAINING_*` env; tree params, seed, split fractions, `max_rows`,
  `as_of`, MLflow URIs, promotion criteria), `dataset.py` (labels up to
  `as_of` → Feast `get_historical_features`), `split.py`, `evaluate.py`,
  `provenance.py` (content fingerprint, git SHA), `train.py`,
  `promote.py`, `cli.py` (`amel-train train | promote | show`). 11 unit
  tests, no infra.
- **MLflow 3.16.1 server** (`infra/mlflow/Dockerfile`; Compose `mlflow`,
  host port 5000): Postgres backend in a new `mlflow` DB, artifacts in
  the MinIO `mlflow` bucket proxied by the server, uvicorn `--workers=2`,
  jobs subsystem disabled, `--allowed-hosts`, `mem_limit: 1g`
  (measured 483–534 MiB).
- **`train` one-shot container** (`infra/training/Dockerfile`, profile
  `train`, `mem_limit: 3g`): `make train`, `make promote`,
  `make model-show`.
- **Alembic `0004`**: `ml.model_promotions` audit table +
  `ModelPromotion` model. `infra/postgres/init/03-create-mlflow-db.sql`
  (created by hand on the existing volume — RUNBOOKS.md).
- **Registry**: `higgs_decision_tree` with `candidate` / `champion`
  aliases; versions tagged with git SHA, dataset fingerprint, feature
  view version, test metrics, promotion context.
- `DECISIONS.md` ADR-0006; LEARNING_LOG/RUNBOOKS/MISTAKES/ARCHITECTURE
  updated. 69 → **80 unit tests**.

## Milestone 4 acceptance run

Run 2026-09-19 against the live stack (Milestone 3 state, simulator and
DAG running throughout).

```
Reproducibility (TRAINING_AS_OF=2026-09-19T21:00:00, max_rows=200000, max_depth=8, seed=42):
  run A -> version 2: fingerprint ff72b440727cb949, test_accuracy 0.689787603526583, roc_auc 0.7585938300843371
  run B -> version 3: fingerprint ff72b440727cb949, test_accuracy 0.689787603526583, roc_auc 0.7585938300843371
  REPRODUCIBLE: True (all 13 metrics identical to the last decimal; 199,623 rows after
  dropping 383 labels with no point-in-time feature match); 36.6 s / 35.3 s; peak 873 MiB

Logged per run: 13 params (incl. as_of, dataset_fingerprint, feature_view_version=1, sklearn 1.9.1),
  tags git_sha=a70d233..., 14 metrics, 11 artifacts (confusion matrices JSON+PNG for validation and
  test, feature_importances.csv/.png, dataset_version.json, feature_definitions.json, split_sizes.json,
  tree_shape.json, registration.json), dataset input higgs_training_ff72b440727cb949, model with
  signature (skops); 82 objects in MinIO bucket `mlflow`

Promotion:
  make promote (candidate v3, no champion) -> PROMOTED, exit 0
    reason: test_accuracy 0.6898 >= floor 0.66; test_roc_auc 0.7586 >= floor 0.7; first promotion
    ml.model_promotions row 1: version 3, decided_by chetan, criteria {0.66, 0.70, 0.005}
  weak candidate (TRAINING_MAX_DEPTH=2) -> version 4, test_accuracy 0.6298
  make promote -> REJECTED, exit 2, champion stays v3, reasons:
    test_accuracy 0.6298 < floor 0.66; test_roc_auc 0.6469 < floor 0.7;
    test_accuracy 0.6298 regresses champion 0.6898 by more than 0.005
  make model-show -> {'candidate': '4', 'champion': '3'}

MLflow server: 1,023 MiB / 1 GiB with defaults -> 483 MiB after --workers=2 + jobs disabled
```

**Milestone 4 acceptance verified**: the decision-tree training run is
reproducible (bit-identical metrics across two runs on a pinned
dataset) and the model is registered under `higgs_decision_tree` with
an explicit, criteria-checked, audited champion promotion.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute")

Unchanged from Milestone 0/1/2/3 (re-verify at the start of Milestone 5) —
see git history for the full table. Additions this milestone:
`mlflow==3.16.1` (server image and workspace client), `scikit-learn==1.9.1`,
`python:3.12-slim-bookworm` for the MLflow image.

## Current known failures / gaps

- Promotion compares each candidate's *own* test split against the
  champion's; a fixed, versioned holdout set would make comparisons
  fairer (ADR-0006 trigger).
- `TRAINING_MAX_ROWS=200000` default; full-scale (`0`) not yet timed.
- The `train` container rebuilds on every `make train` (`--build`) —
  ~2 min the first time, cached after; fine locally, wasteful in CI.
- MLflow is a single server: inference (Milestone 5) must cache the
  champion and tolerate MLflow being down after start-up.
- Postgres init scripts only run on a fresh volume: new databases
  (`mlflow` this milestone) must be created by hand on existing
  volumes (RUNBOOKS.md).
- Carried: simulator position manual on restart; synchronous per-run
  materialization; `higgs_features_flat` plain VIEW; training-dataset
  scan growth; download hardening; no CI (Milestone 7); no dedicated
  ingestor failure script.

## Commands that work today

```bash
make install       # uv sync --all-packages
make lint / fmt / typecheck / test   # all pass, no infra required (80 tests)

make up             # postgres, kafka, minio, redis, airflow, feast-server, mlflow + one-shots
make logs / make down
make smoke          # Milestone 1
make feast-materialize / make feast-demo   # Milestone 3
make failure-simulator-wedge

# Training + MLflow (Milestone 4):
TRAINING_AS_OF=2026-09-19T21:00:00 make train    # pinned, reproducible; omit AS_OF for "now"
TRAINING_MAX_DEPTH=2 make train                  # any TRAINING_* knob passes through
DECIDED_BY=you make promote                      # exit 0 promoted / exit 2 rejected
make promote PROMOTE_ARGS="--version 3"          # explicit version
make promote PROMOTE_ARGS=--force                # recorded as forced
make model-show
docker exec amel-postgres-1 psql -U amel -d amel -c "SELECT * FROM ml.model_promotions ORDER BY id DESC"
# UI: http://localhost:5000

# Feast / probes / Airflow: unchanged from Milestone 3 (see MILESTONE_REPORT.md)
```

## Next task

**Milestone 5 — FastAPI inference.**

Acceptance: `apps/inference_api` loads the champion model
(`models:/higgs_decision_tree@champion`) and returns predictions with
model metadata. Endpoints per AMEL_KICKOFF_PROMPT.md: `GET /health`,
`GET /ready`, `POST /predict/raw` (validated 28 features),
`POST /predict/entity/{entity_id}` (features via Feast online /
`feast-server`), `GET /model`, `GET /metrics`. Cache the model safely
with a controlled refresh; persist prediction metadata (Postgres, `ml`
schema); publish prediction events to Kafka; return prediction id +
model version. Honest liveness/readiness from the start (model loaded,
feast-server reachable). Memory: one more always-on container; the
skops-loaded tree is small — cap at 1 GB.
