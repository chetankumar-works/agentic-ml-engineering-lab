#!/usr/bin/env bash
# Remove KFP from the kind cluster, restore the 3 GB node cap and the
# Compose services stopped by scripts/kfp_up.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f infra/docker-compose.yml"
KFP_VERSION="${KFP_VERSION:-2.17.2}"
kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=${KFP_VERSION}" --ignore-not-found >/dev/null 2>&1 || true
kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=${KFP_VERSION}" --ignore-not-found >/dev/null 2>&1 || true
docker update --memory 3g --memory-swap 3g amel-control-plane >/dev/null
$COMPOSE up -d --no-deps airflow grafana loki tempo >/dev/null 2>&1
echo "KFP removed; node cap 3g; airflow/grafana/loki/tempo back"
