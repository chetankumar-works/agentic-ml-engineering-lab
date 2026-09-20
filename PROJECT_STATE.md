# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 7 — Docker hardening + CI/CD.** Complete.

## Completed work (Milestone 7; earlier milestones in MILESTONE_REPORT.md)

- **Compose hardening**: every image tag pinned (MinIO
  `RELEASE.2025-09-07T16-13-09Z` / mc `RELEASE.2025-08-13T08-35-41Z`,
  Tempo 3.0.0, Loki 3.7.8, Prometheus v3.14.0, Grafana 13.2.2,
  kafka-exporter v1.10.0, collector 0.161.0); `restart: unless-stopped`
  on all 16 long-running services; healthchecks on Prometheus and
  Grafana (Tempo/Loki/collector are distroless — documented); non-root
  `USER` in feast, training, migrate and mlflow images (all first-party
  images now non-root); tighter `.dockerignore`;
  `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false`.
- **`scripts/make_synthetic_higgs.py`**: synthetic `HIGGS.csv.gz`-in-zip
  with the real packaging; streamed by the simulator unchanged (test).
- **`.github/workflows/ci.yml`**: quality (ruff, format, mypy, pytest) ·
  compose config · Trivy CRITICAL on `uv.lock` · 8 image builds
  (`ghcr.io/chetankumar-works/agentic-ml-engineering-lab/<image>:<sha>`,
  pushed on main/tags) · integration (Postgres + Kafka + simulator +
  ingestor on the synthetic archive → M1 smoke → probes).
- `DECISIONS.md` ADR-0009; docs updated. 89 → **90 unit tests**.

## Milestone 7 acceptance run

```
Clean checkout (2026-09-20, main stack stopped, fresh clone into a scratch dir,
COMPOSE_PROJECT_NAME=amelclean => fresh volumes):
  git clone + make install            1.5 s (cached uv)
  make_synthetic_higgs --rows 20000   2,111,682 bytes
  make up                             2 m 41 s; 20 containers; init scripts created airflow/feast/mlflow;
                                      alembic 0005; 9 fresh volumes
  inference-api before any model      /health 200, /ready 503 "no model loaded: ... not found"  (honest)
  make smoke (min 5000)               5,391 rows / 5,391 distinct; DLQ 15 + 17  PASSED
  DAG (after unpausing — bug found)   78,432 curated rows; update_feature_store windows=4; Redis 78,432
  make feast-demo                     PASSED (20/20 features; 0/20 leaked; online 5/5)
  make train                          v1, test_accuracy 0.7879 (synthetic data), fingerprint a76d1d495856d691
  make promote                        PROMOTED (first champion)
  POST /model/refresh                 {"previous_version":null,"current_version":"1","swapped":true}
  /ready                              200 ready
  make smoke-tracing                  PASSED (after fixing a ~30 s span-metrics race)
  memory                              ≈ 7.0 GiB for the whole stack
  teardown                            down -v; 0 amelclean volumes left

GitHub Actions (run 35531470319, main, 2026-09-20 19:11 -> 19:14, success):
  quality 48 s · compose config 7 s · trivy 12 s (uv.lock: 0 CRITICAL) ·
  8 image builds 18–40 s each (GHA cache) pushed to GHCR by SHA ·
  integration 1 m 54 s: M1 smoke 5,738 rows / 0 duplicates / DLQ 26 + 40, /ready 200 on both services
  (two earlier runs failed on stale action pins — fixed)
```

**Milestone 7 acceptance verified**: a clean checkout starts the local
environment with the documented commands (README "Clean checkout, start
to finish"), and GitHub Actions passes on the pushed branch.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute")

Unchanged from Milestone 0–6 (re-verify at the start of Milestone 8) —
see git history for the full table. Additions this milestone:
`opentelemetry-sdk 1.44.0` / instrumentations `0.65b0`; images
`otel/opentelemetry-collector-contrib:0.161.0`, `grafana/tempo` 3.0.0,
`grafana/loki` 3.7.8, `prom/prometheus` 3.14.0, `grafana/grafana` 13.2.2,
`danielqsj/kafka-exporter` 1.10.0 — all pinned in Compose as of Milestone 7. GitHub Actions: `actions/checkout@v7`,
`astral-sh/setup-uv@v7`, `aquasecurity/trivy-action@v0.36.0`,
`docker/build-push-action@v7`, `docker/setup-buildx-action@v4`,
`docker/login-action@v4`.

## Current known failures / gaps

- CI's integration job covers ingestion only (runner memory); Feast/
  MLflow/inference are proven by the manual clean-checkout run.
- Images are `linux/amd64` only; Trivy scans the lockfile, not images.
- Tempo/Loki/collector have no in-image healthcheck (distroless).
- The clean-checkout procedure is manual (documented in README/
  LEARNING_LOG); a scripted `make clean-checkout-test` is a candidate.
- Carried: shared admin token; synchronous persist/publish; promotion
  on own test split; simulator position manual; Redis growth; third-
  party container logs not in Loki; no alert rules.

## Commands that work today

```bash
make install · make lint / fmt / typecheck / test (90 tests)
uv run python scripts/make_synthetic_higgs.py   # synthetic dataset for a quick or clean start
make up / logs / down
make smoke · make feast-materialize · make feast-demo · make failure-simulator-wedge
make train · make promote DECIDED_BY=you · make model-show · make smoke-tracing
gh run list --limit 3 · gh run watch <id>
# Clean checkout: README "Clean checkout, start to finish"
```

## Next task

**Milestone 8 — Kubernetes.** BLOCKED ON A DECISION: present the
kind/minikube memory plan to the developer before starting (their
explicit instruction). Acceptance: core stateless services run on
kind/minikube with health probes and configuration separation.
Candidates for "core stateless": inference-api, feast-server,
source-simulator, stream-ingestor (Postgres/Kafka/Redis/MinIO/MLflow
stay in Compose, reached from the cluster via the host). Manifests
under `infra/k8s/`, validated in CI with `kubeconform`; ConfigMaps/
Secrets for configuration; liveness/readiness probes on the honest
`/health` and `/ready` endpoints built in Milestones 3 and 5.
