#!/usr/bin/env bash
# Bring the AMEL kind cluster up next to the Compose stack (Milestone 8,
# DECISIONS.md ADR-0010). Idempotent. Steps:
#   1. stop the four Compose services that move into the cluster
#   2. create the kind cluster on the Compose network, cap the node at 3g
#   3. install ingress-nginx (pinned) and load the locally built images
#   4. create the Secret from env (never from a committed file), the
#      simulator's HIGGS_START_INDEX ConfigMap, apply the manifests,
#      run the migrate + feast-apply Jobs, wait for rollouts
#   5. point Compose's Airflow at the in-cluster feast-server NodePort
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f infra/docker-compose.yml"
NODE_MEMORY="${KIND_NODE_MEMORY:-3g}"
INGRESS_NGINX_VERSION="${INGRESS_NGINX_VERSION:-controller-v1.15.1}"
MOVED="inference-api feast-server source-simulator stream-ingestor"

echo "== 1. stopping the Compose services that move into the cluster"
$COMPOSE stop $MOVED >/dev/null 2>&1 || true

echo "== 2. kind cluster 'amel' on the Compose network"
if ! kind get clusters 2>/dev/null | grep -qx amel; then
  KIND_EXPERIMENTAL_DOCKER_NETWORK=amel_default kind create cluster \
    --config infra/k8s/kind-config.yaml --wait 120s
else
  echo "   already exists"
  # A stopped node may hold KFP; starting it here at the 3g cap would
  # livelock it (ADR-0012). mode.sh starts it inside the kfp envelope.
  [ "$(docker inspect -f '{{.State.Running}}' amel-control-plane)" = true ] \
    || { echo "node is stopped: use make mode-k8s" >&2; exit 1; }
  ns=$(kubectl get namespace kubeflow --ignore-not-found -o name)
  [ -z "$ns" ] || { echo "KFP is installed: run make kfp-down first" >&2; exit 1; }
  # Step 2 sets the 3g cap; under scale mode's workload that squeezes the node.
  [ "$(docker inspect -f '{{.HostConfig.Memory}}' amel-control-plane)" = "$(numfmt --from=iec "${NODE_MEMORY^^}")" ] \
    || { echo "node is not at the $NODE_MEMORY cap (another mode): use make mode-k8s" >&2; exit 1; }
fi
docker update --memory "$NODE_MEMORY" --memory-swap "$NODE_MEMORY" amel-control-plane >/dev/null
echo "   node capped at $NODE_MEMORY"

echo "== 3. ingress-nginx ($INGRESS_NGINX_VERSION) + images"
kubectl apply -f "https://raw.githubusercontent.com/kubernetes/ingress-nginx/${INGRESS_NGINX_VERSION}/deploy/static/provider/kind/deploy.yaml" >/dev/null
# the Ingress below is validated by ingress-nginx's admission webhook — it must be up first
kubectl -n ingress-nginx wait --for=condition=available deployment/ingress-nginx-controller --timeout=180s >/dev/null
# ingress-nginx runs one worker per CPU by default: 32 here, 322 MiB anon.
# Two workers bound it internally (37 MiB); the limit is a backstop with
# room for its binaries' page cache (DECISIONS.md ADR-0012).
kubectl -n ingress-nginx patch configmap ingress-nginx-controller --type merge \
  -p '{"data":{"worker-processes":"2"}}' >/dev/null
kubectl -n ingress-nginx set resources deployment/ingress-nginx-controller --limits=memory=256Mi >/dev/null
kubectl -n ingress-nginx rollout status deployment/ingress-nginx-controller --timeout=180s >/dev/null
$COMPOSE build -q $MOVED migrate >/dev/null
for img in amel-inference-api amel-feast-server amel-source-simulator amel-stream-ingestor amel-migrate; do
  kind load docker-image "$img:latest" --name amel >/dev/null 2>&1 && echo "   loaded $img"
done

echo "== 4. secrets, config, manifests, jobs"
kubectl apply -f infra/k8s/base/namespace.yaml >/dev/null
kubectl -n amel create secret generic amel-secrets \
  --from-literal=DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://amel:amel_dev_password@postgres:5432/amel}" \
  --from-literal=INFERENCE_ADMIN_TOKEN="${INFERENCE_ADMIN_TOKEN:-dev-admin-token}" \
  --from-literal=AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-amel}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-amel_dev_password}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
# The simulator restarts its entity_id counter; resume past what has landed (RUNBOOKS.md).
MAXID=$(docker exec amel-postgres-1 psql -U amel -d amel -tAc \
  "SELECT coalesce(max(entity_id),'higgs-000000000') FROM landing.higgs_feature_events" 2>/dev/null || echo higgs-000000000)
START=$(( 10#${MAXID#higgs-} + 1 ))
kubectl -n amel create configmap simulator-runtime --from-literal=HIGGS_START_INDEX="$START" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "   HIGGS_START_INDEX=$START"
kubectl apply -k infra/k8s/base >/dev/null
for job in migrate feast-apply; do
  kubectl -n amel delete job "$job" --ignore-not-found >/dev/null
  kubectl apply -f "infra/k8s/jobs/$job.yaml" >/dev/null
done
kubectl -n amel wait --for=condition=complete job/migrate job/feast-apply --timeout=300s
for d in feast-server inference-api source-simulator stream-ingestor; do
  kubectl -n amel rollout status deployment/"$d" --timeout=300s
done

echo "== 5. Airflow -> in-cluster feast-server"
FEAST_SERVER_URL=http://amel-control-plane:30566 $COMPOSE up -d --no-deps airflow >/dev/null 2>&1
echo
kubectl -n amel get pods -o wide
echo
echo "inference API: http://localhost:8003 (NodePort) and http://amel.localtest.me/ (Ingress)"
