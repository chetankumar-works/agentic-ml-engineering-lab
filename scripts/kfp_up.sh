#!/usr/bin/env bash
# Kubeflow Pipelines standalone on the AMEL kind cluster (Milestone 9,
# DECISIONS.md ADR-0011). Memory plan: KFP's control plane (API server,
# MySQL, MinIO, metadata, workflow controller, ...) needs ~2 GB, so the
# node cap goes 3 -> 6 GB and Compose's Airflow + Grafana/Loki/Tempo
# (~2.4 GB, not needed while training on KFP) are stopped first.
# `scripts/kfp_down.sh` reverses it.
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f infra/docker-compose.yml"
KFP_VERSION="${KFP_VERSION:-2.17.2}"
NODE_MEMORY="${KIND_NODE_MEMORY_KFP:-6g}"

kind get clusters | grep -qx amel || { echo "run make k8s-up first"; exit 1; }

echo "== 1. free memory: stopping airflow, grafana, loki, tempo (Compose)"
$COMPOSE stop airflow grafana loki tempo >/dev/null 2>&1 || true
docker update --memory "$NODE_MEMORY" --memory-swap "$NODE_MEMORY" amel-control-plane >/dev/null
echo "   node cap now $NODE_MEMORY"

echo "== 2. KFP $KFP_VERSION standalone (cluster-scoped resources, then platform-agnostic env)"
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=${KFP_VERSION}" >/dev/null
kubectl wait --for=condition=established --timeout=120s crd/applications.app.k8s.io >/dev/null
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=${KFP_VERSION}" >/dev/null

echo "== 3. training image into the cluster"
$COMPOSE --profile train build -q train >/dev/null
kind load docker-image amel-training:local --name amel >/dev/null 2>&1 && echo "   loaded amel-training:local"

echo "== 4. waiting for the KFP control plane"
for d in ml-pipeline ml-pipeline-ui workflow-controller metadata-grpc-deployment mysql seaweedfs; do
  kubectl -n kubeflow rollout status deployment/"$d" --timeout=600s >/dev/null && echo "   $d ready"
done
# our task pods need the Compose-side secrets, in the namespace KFP runs pipelines in (kubeflow)
kubectl -n kubeflow create secret generic amel-secrets \
  --from-literal=DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://amel:amel_dev_password@postgres:5432/amel}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo
kubectl -n kubeflow get pods | grep -vE 'Completed'
echo
echo "UI/API: kubectl -n kubeflow port-forward svc/ml-pipeline-ui 8888:80   then http://localhost:8888"
