.PHONY: install lint fmt typecheck test up down logs migrate seed smoke feast-materialize feast-demo failure-simulator-wedge train promote model-show smoke-tracing mode mode-compose mode-k8s mode-kfp k8s-up k8s-down k8s-status k8s-validate pipeline-compile pipeline-run-steps pipeline-run-docker kfp-up kfp-down kfp-submit

COMPOSE = docker compose -f infra/docker-compose.yml

install:
	uv sync --all-packages

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

typecheck:
	uv run mypy .

test:
	uv run pytest

# --- Infra-dependent targets (Milestone 1: Postgres + Kafka + source_simulator + stream_ingestor) ---

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

migrate:
	$(COMPOSE) run --rm migrate

seed:
	@echo "no separate seed step: landing data arrives continuously from source_simulator once 'make up' is running"

smoke:
	uv run python scripts/smoke_milestone1.py

# --- Feature store (Milestone 3: Feast + Redis) ---

feast-materialize:   # chunked backfill of curated -> Redis; resumes from the registry
	$(COMPOSE) --profile feast-demo run --rm --build feast-materialize

feast-demo:          # historical (point-in-time) + online retrieval acceptance demo
	$(COMPOSE) --profile feast-demo run --rm --build feast-demo

# --- Failure engineering (regression scripts for real incidents, see RUNBOOKS.md) ---

failure-simulator-wedge:   # pauses the Kafka broker; simulator probes must go 503 and recover
	uv run python scripts/failure_engineering/simulator_producer_wedge.py

# --- Training + MLflow (Milestone 4) ---

GIT_SHA := $(shell git rev-parse HEAD 2>/dev/null)

train:               # one reproducible training run -> MLflow run + registered version aliased `candidate`
	GIT_SHA=$(GIT_SHA) $(COMPOSE) --profile train run --rm --build train amel-train train

promote:             # explicit, audited candidate -> champion (fails with exit 2 if criteria fail)
	$(COMPOSE) --profile train run --rm train amel-train promote --decided-by "$(or $(DECIDED_BY),$(USER))" $(PROMOTE_ARGS)

model-show:          # current candidate/champion from the registry
	$(COMPOSE) --profile train run --rm train amel-train show

# --- Observability (Milestone 6) ---

smoke-tracing:       # one prediction traced end to end: response -> Tempo -> Loki -> Postgres
	uv run python scripts/smoke_milestone6.py

# --- Compose/kind modes (DECISIONS.md ADR-0012): never the full stack and the cluster together ---

mode:                # current mode and any violation
	./scripts/mode.sh status

mode-compose:        # stop the kind node, full Compose stack
	./scripts/mode.sh compose

mode-k8s:            # node at 3g (KFP removed first if present), Compose minus the four moved services
	./scripts/mode.sh k8s

mode-kfp:            # node at 6g, Compose trimmed to postgres/minio/mlflow, amel Deployments at 0
	./scripts/mode.sh kfp

# --- Kubernetes on kind (Milestone 8) ---

k8s-up:              # kind cluster next to Compose; moves inference-api, feast-server, simulator, ingestor
	./scripts/k8s_up.sh

k8s-down:            # delete the cluster, hand the four services back to Compose
	./scripts/k8s_down.sh

k8s-status:
	kubectl -n amel get deploy,pods,svc,ingress,jobs
	docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' amel-control-plane

k8s-validate:        # schema-validate every manifest (also runs in CI)
	kubeconform -strict -summary -ignore-missing-schemas infra/k8s/base infra/k8s/jobs infra/k8s/secret.example.yaml

# --- Kubeflow Pipelines v2 (Milestone 9) ---

pipeline-compile:    # PipelineSpec IR -> ml/pipelines/compiled/higgs_training_pipeline.yaml
	uv run amel-pipeline compile

pipeline-run-steps:  # the six steps in-process inside the training container (no Kubeflow) — reference path
	$(COMPOSE) --profile train run --rm --build train amel-pipeline run-steps --max-rows $(or $(TRAINING_MAX_ROWS),100000) $(if $(TRAINING_AS_OF),--as-of $(TRAINING_AS_OF),)

pipeline-run-docker: # compiled components, one container per task, via kfp.local DockerRunner
	uv run amel-pipeline run-docker --max-rows $(or $(TRAINING_MAX_ROWS),100000) $(if $(TRAINING_AS_OF),--as-of $(TRAINING_AS_OF),) --git-sha $(GIT_SHA)

kfp-up:              # KFP 2.17 standalone on the kind cluster (needs: make mode-kfp)
	./scripts/kfp_up.sh

kfp-down:            # remove KFP, verify the namespace is gone, node cap back to 3g
	./scripts/kfp_down.sh

kfp-submit:          # submit to the in-cluster API server (needs: kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8888:80)
	uv run amel-pipeline submit --host $(or $(KFP_HOST),http://localhost:8888) --max-rows $(or $(TRAINING_MAX_ROWS),100000) $(if $(TRAINING_AS_OF),--as-of $(TRAINING_AS_OF),) --git-sha $(GIT_SHA)
