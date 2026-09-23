#!/usr/bin/env bash
# Remove KFP from the kind cluster and restore the 3 GB node cap. Compose
# is left as it is (the kfp set); `make mode-k8s` / `make mode-compose`
# brings the rest back.
#
# The cap is lowered only after the kubeflow namespace is verified gone.
# A failed delete used to be swallowed (`|| true`), the cap dropped to
# 3 GB with KFP still installed, and the node livelocked at its limit
# (MISTAKES.md, Milestone 9; DECISIONS.md ADR-0012). Any failure here
# exits non-zero with the node still at its KFP cap.
set -euo pipefail
cd "$(dirname "$0")/.."
KFP_VERSION="${KFP_VERSION:-2.17.2}"
NODE_MEMORY="${KIND_NODE_MEMORY:-3g}"
NS_GONE_TIMEOUT="${KFP_NS_GONE_TIMEOUT:-300}"

kind get clusters | grep -qx amel || { echo "no kind cluster 'amel'; nothing to remove"; exit 1; }

echo "== 1. deleting KFP $KFP_VERSION (platform-agnostic env, then cluster-scoped resources)"
kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=${KFP_VERSION}" \
  --ignore-not-found --wait=false >/dev/null
# The kubeflow Namespace object lives in cluster-scoped-resources, so the
# namespace only starts terminating after this second delete.
kubectl delete -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=${KFP_VERSION}" \
  --ignore-not-found --wait=false >/dev/null

echo "== 2. waiting up to ${NS_GONE_TIMEOUT}s for namespace kubeflow to be gone"
# --ignore-not-found: empty output means gone; a non-zero exit (API
# unreachable, auth) must abort, not read as "gone". The bare assignment
# is deliberate: set -e ignores a failing $(...) inside `[ ... ]`.
deadline=$(( SECONDS + NS_GONE_TIMEOUT ))
while :; do
  ns=$(kubectl get namespace kubeflow --ignore-not-found -o name)
  [ -z "$ns" ] && break
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "namespace kubeflow still present after ${NS_GONE_TIMEOUT}s; node cap NOT lowered" >&2
    kubectl get namespace kubeflow -o jsonpath='{.status.phase}{" "}{.status.conditions[*].message}{"\n"}' >&2 || true
    kubectl -n kubeflow get pods --no-headers 2>/dev/null | head -20 >&2 || true
    exit 1
  fi
  sleep 5
done
echo "   gone"

echo "== 3. node cap -> $NODE_MEMORY"
docker update --memory "$NODE_MEMORY" --memory-swap "$NODE_MEMORY" amel-control-plane >/dev/null

echo "KFP removed; node cap $NODE_MEMORY. Compose is still the kfp set:"
echo "next: make mode-k8s or make mode-compose (DECISIONS.md ADR-0012)"
