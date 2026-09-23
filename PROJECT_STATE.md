# Project State

Read this first when resuming work on AMEL, with or without prior
conversational history. Updated at the end of every milestone (and at any
clean stopping point mid-milestone) per the Definition of Done in
`AMEL_KICKOFF_PROMPT.md`.

## Current milestone

**Milestone 9 — Kubeflow Pipelines v2.** Complete (tag `milestone-9`).
**Milestone 10 has not started**: its memory budget (DECISIONS.md
ADR-0012, "Projected budget for M10") needs approval first.

**Machine state at close:** `k8s` mode (`make mode`). The kind node is
at 3g with KFP removed, the M8 services are running, and Compose is
everything except the four moved services.

## Completed work (Milestone 9; earlier milestones in MILESTONE_REPORT.md)

- **`ml/training/src/amel_training/steps.py`**: six file-to-file steps
  plus `run_all`, calling what `train.py` calls.
- **`ml/pipelines/`** (`amel-pipeline`): components on
  `amel-training:local`, `higgs_training_pipeline`, compiled IR, and the
  `run-steps` / `run-docker` / `submit` runners.
- **KFP on kind**: `scripts/kfp_up.sh` (runs only in kfp mode) and
  `scripts/kfp_down.sh` (deletes both kustomizations, verifies the
  `kubeflow` namespace is gone, then lowers the cap to 3g; fails safe
  otherwise).
- **Modes (ADR-0012)**: `scripts/mode.sh`, `make mode | mode-compose |
  mode-k8s | mode-kfp`. The full Compose stack and the node never run
  together. The node cap identifies the mode, and guards sit in the up
  scripts.
- **Postgres bounded by its own settings** (Compose `command:`):
  max_connections 50, max_parallel_workers 2, jit off, the rest
  explicit.
- **`scripts/mem_sample.sh`**: read-only cgroup sampler (VM, node, pods,
  Compose), down to 250 ms.
- 92 → **96 unit tests**.

## Milestone 9 acceptance run

```
make mode-kfp                      20 s: Compose trimmed to postgres/minio/mlflow, node cap 6g, amel Deployments -> 0
make kfp-up                        1m22s (mode check passed; KFP 2.17.2, 14 pods Running)
make kfp-submit                    run 08e728c7-d886-4fe9-8a2f-4a9aecbccedb SUCCEEDED in 3m46s
  MLflow                           run kfp-08e728c7-... FINISHED; params orchestrator=kfp, dataset_n_rows 99805;
                                   tags kfp_run_id, git_sha 493c014; 13 metrics (test acc 0.6885, ROC-AUC 0.7540);
                                   11 artifacts; higgs_decision_tree v12 @candidate (champion v5 unchanged)
  memory (2 s samples)             node anon 1.97 -> 2.92 GiB peak (cap 6), full PSI 0.00; Postgres anon+shmem 0.17 GiB;
                                   VM MemAvailable >= 10.1 GB throughout
make kfp-down                      47 s: namespace verified gone, cap -> 3g
make mode-k8s                      5m33s: M8 services rolled out, Airflow -> in-cluster feast; VM 7.16 GB available
Earlier (same code): train v6 / run-steps v7 / run-docker v8 -> fingerprint 81f8a8fd191441aa, identical metrics
```

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
`controller-v1.15.1`. Milestone 9 (re-verified 2026-09-23):
Python 3.12.14, uv 0.12.16, Docker Compose v5.4.0, kfp SDK 2.17.0 +
kfp-kubernetes 2.17.0, KFP backend 2.17.2 (standalone,
platform-agnostic manifests), mlflow 3.16.1, scikit-learn 1.9.1,
librdkafka 2.15.1 (via confluent-kafka), Postgres 16.15. Pinned for the
M10 budget: KEDA v2.21.0, metrics-server v0.9.0 (not installed).

## Current known failures / gaps

- **M10 does not fit the 3 GiB k8s node** (projected 3.24–3.79 GiB
  anon). The proposed `scale` mode (4.5 GiB cap, no Airflow, bounded
  librdkafka and pools) is awaiting approval (ADR-0012).
- Unbounded consumers: MinIO (all modes); Redis (no `maxmemory`, 1.38
  GiB used, **425 MiB in swap**), Airflow (1.28 GiB) and ingress-nginx
  (322 MiB) in k8s mode.
- The 75% cap margin is a heuristic, not a measured boundary.
- 28 `max` events on the node during the kfp run: re-check with 250 ms
  sampling on the next KFP training run.
- KFP brings its own MySQL and SeaweedFS; `amel-secrets` is duplicated
  into `kubeflow`; the `:local` image tag works only on this machine.
- Carried from M8: env keys duplicated across Compose and k8s; a stalled
  ingestor is restarted, not fixed; sampled RED for the two Kafka-heavy
  services; shared admin token; synchronous persist/publish; promotion
  on its own test split; third-party logs not in Loki; no alerts; CI
  integration covers ingestion only.

## Commands that work today

```bash
make install · make lint / fmt / typecheck / test (96 tests)
make mode                                          # current mode or violations — run this first after any Docker restart
make mode-compose · make mode-k8s · make mode-kfp  # the only way to switch (ADR-0012)
make kfp-up · make kfp-submit · make kfp-down      # kfp mode; kfp-submit needs: kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8888:80
make pipeline-compile · pipeline-run-steps · pipeline-run-docker
make k8s-status · make k8s-validate
make smoke · make feast-demo · make train · make promote DECIDED_BY=you · make smoke-tracing
scripts/mem_sample.sh /tmp/r.tsv 300 2 0.25 60     # memory sampling (budget from anon+shmem, never docker stats)
```

## Next task

**Milestone 10 — HPA + KEDA scaling experiment.** Acceptance: inference
can scale and Kafka consumer scaling can be demonstrated.
**Blocked on approval of the M10 memory budget** (ADR-0012). Once it is
approved, the first steps measure before anything scales out:
1. Bound the ingestor's librdkafka queue and fetch sizes, and the
   SQLAlchemy pools of the scaled services.
2. Add a `scale` mode to `mode.sh`.
3. Measure one ingestor draining a deliberate backlog (250 ms sampling).
4. Install metrics-server and KEDA with explicit limits, and measure
   them.
5. Only then raise the maximum replica counts.
