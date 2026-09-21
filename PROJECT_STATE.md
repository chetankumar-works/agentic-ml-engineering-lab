# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 8 — Kubernetes (kind).** Complete.

## Completed work (Milestone 8; earlier milestones in MILESTONE_REPORT.md)

- **`infra/k8s/`**: `kind-config.yaml` (1 node, dataset hostPath mount,
  host ports 80/8001/8002/8003/6566), `base/` (Namespace `amel`,
  ConfigMap `amel-config`, Deployments + NodePort Services for
  `inference-api`, `feast-server`, `source-simulator`, `stream-ingestor`
  with startup/liveness/readiness probes, requests/limits, non-root
  securityContext, `maxUnavailable: 0`; Ingress `amel.localtest.me`;
  kustomization), `jobs/` (`migrate`, `feast-apply`),
  `secret.example.yaml` (template — the real Secret is created from env).
- **`scripts/k8s_up.sh` / `k8s_down.sh`** (`make k8s-up/down/status/
  validate`): cluster on the Compose network, node capped at 3 GB,
  ingress-nginx `controller-v1.15.1`, images via `kind load`, Secret +
  `simulator-runtime` ConfigMap (`HIGGS_START_INDEX`), Jobs, rollouts,
  Airflow re-pointed at the in-cluster feast-server.
- **Stall watchdog** in `stream_ingestor` and `source_simulator`
  liveness (`stall_timeout_seconds`, default 120 s) — found necessary
  by the probe drill (a frozen Postgres hung the loop with `/health`
  200). 2 new unit tests.
- **Trace sampling** for the two Kafka-heavy services (2%,
  `OTEL_KAFKA_TRACE_RATIO`) after Tempo OOM-looped; Tempo 768 MB.
- **Prometheus** scrapes Compose names *and* kind NodePorts
  (`runtime` label); `smoke_milestone6.py` requires one target up per job.
- **CI**: `kubeconform` + `kubectl kustomize` job. Tools: kind v0.33.0,
  kubeconform v0.8.0 (in `~/.local/bin`), kubectl v1.36.1, cluster
  v1.37.0. 90 → **92 unit tests**.

## Milestone 8 acceptance run

Run 2026-09-20/21 (option (a) from the memory plan: kind alongside
Compose, 3 GB node cap).

```
make k8s-up                        57 s (cluster pre-created earlier: kind create ~40 s)
  pods                             feast-server, inference-api, source-simulator, stream-ingestor 1/1 Running;
                                   migrate + feast-apply Jobs Completed
  DNS from a pod                   kafka:9092 reachable, postgres:5432 reachable (Compose network)
  NodePort                         localhost:8003/ready -> 200; /model -> v5 @champion
  Ingress                          http://amel.localtest.me/model -> 200 (ingress-nginx, hostPort 80)
  probes (kubectl describe)        Startup /health 5s×24 · Liveness /health 10s×3 · Readiness /ready 10s×2
  in-cluster ingestion             simulator start_index 2097438; ingestor assigned; landing 2,100,393 -> 2,149,789
  Airflow -> in-cluster feast      FEAST_SERVER_URL=http://amel-control-plane:30566; DAG 10/10, materialized windows=1
  make smoke-tracing               PASSED against the NodePort (13 spans, inference_api + feast_server)

Probe drills:
  bad config rollout               set INFERENCE_MLFLOW_TRACKING_URI=http://nowhere:5000 ->
                                   new pod Ready=false ("no model loaded: ... nowhere:5000"), old pod serving v5,
                                   rollout status blocked; rollout undo -> 1 ready pod
  frozen Postgres (pause)          BEFORE fix: no restart, no log line for the whole window, /health 200 (lie)
                                   AFTER stall watchdog: /health "503 consume loop stalled for 67s" ->
                                   kubelet "Killing ... failed liveness probe" -> 3 restarts in ~6 min ->
                                   Ready=true seconds after unpause, ingestion resumed

Memory:
  kind node                        anon 1.44 GB (4 pods) -> 1.72 GB (+ingress-nginx, after drills);
                                   docker stats 2.3–2.5 GB incl. image page cache; cap 3 GB
  host                             8.1 GB used before -> 8.9 GB with the cluster (6.9 GB available);
                                   all containers 10.3 GB by docker stats
  Tempo                            113 restarts at 512 MB with 100% Kafka spans -> 0 restarts, ~100 MB after 2% sampling
```

**Milestone 8 acceptance verified**: the core stateless services run
on kind with health probes that provably act (rollout gating, liveness
restart) and with configuration separated into ConfigMap/Secret.

## Verified tool versions (WSL2, Ubuntu 26.04 LTS "resolute")

Unchanged from Milestone 0–7 (re-verify at the start of Milestone 9) —
see git history for the full table. Additions this milestone:
`opentelemetry-sdk 1.44.0` / instrumentations `0.65b0`; images
`otel/opentelemetry-collector-contrib:0.161.0`, `grafana/tempo` 3.0.0,
`grafana/loki` 3.7.8, `prom/prometheus` 3.14.0, `grafana/grafana` 13.2.2,
`danielqsj/kafka-exporter` 1.10.0 — all pinned in Compose as of Milestone 7. GitHub Actions: `actions/checkout@v7`,
`astral-sh/setup-uv@v7`, `aquasecurity/trivy-action@v0.36.0`,
`docker/build-push-action@v7`, `docker/setup-buildx-action@v4`,
`docker/login-action@v4`. Milestone 8: kind v0.33.0 (cluster
Kubernetes v1.37.0), kubeconform v0.8.0, kubectl v1.36.1, ingress-nginx
`controller-v1.15.1`.

## Current known failures / gaps

- Compose and k8s carry the same env keys in two places (drift risk).
- NodePorts reuse Compose host ports: the two runtimes are mutually
  exclusive for the four moved services (by design, documented).
- No metrics-server / `kubectl top` yet (Milestone 10 needs it for HPA).
- A stalled ingestor is restarted, not fixed — the dependency still has
  to be repaired by the operator (runbook).
- Simulator/ingestor RED metrics from spans are sampled (2%); their own
  `/metrics` counters remain exact.
- Docker page cache inflates the kind node's apparent memory.
- Carried: shared admin token; synchronous persist/publish; promotion
  on own test split; Redis growth; third-party logs not in Loki; no
  alerts; CI integration covers ingestion only.

## Commands that work today

```bash
make install · make lint / fmt / typecheck / test (92 tests)
make up / logs / down                              # Compose (all 20 containers)
make k8s-up                                        # kind next to Compose; moves the 4 stateless services
make k8s-status · make k8s-validate · make k8s-down
kubectl -n amel get pods,svc,ingress,jobs · kubectl -n amel get events --sort-by=.lastTimestamp
curl -s localhost:8003/model · curl -s amel.localtest.me/ready
make smoke · make feast-demo · make train · make promote DECIDED_BY=you · make smoke-tracing   # all work in either runtime
docker stats --no-stream amel-control-plane; docker exec amel-control-plane cat /sys/fs/cgroup/memory.stat | grep ^anon
```

## Next task

**Milestone 9 — Kubeflow Pipelines v2.** Acceptance: training executes
as containerized KFP components and records results in MLflow.
Components per the kickoff: `load_training_dataset`,
`validate_training_dataset`, `split_dataset`, `train_model`,
`evaluate_model`, `register_model`; compile to YAML; document how KFP
turns components into Kubernetes workloads. Memory plan first: a full
KFP install (pipelines API server, MySQL, MinIO, Argo/workflow
controller, metadata service) is ~2–3 GB on top of the 3 GB kind cap —
options are (i) raise the node cap and stop Airflow/Grafana while KFP
runs, (ii) the lighter "KFP standalone" deployment, or (iii) compile the
pipeline and run components with a local runner while proving the
container images and the MLflow recording. Decide and measure before
installing; the training container image already exists.
