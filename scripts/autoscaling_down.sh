#!/usr/bin/env bash
# Remove KEDA and metrics-server (scripts/autoscaling_up.sh), e.g. before
# leaving scale mode: k8s mode's 3g budget does not include them.
set -euo pipefail
cd "$(dirname "$0")/.."
METRICS_SERVER_VERSION="${METRICS_SERVER_VERSION:-v0.9.0}"
KEDA_VERSION="${KEDA_VERSION:-v2.21.0}"
# ScaledObjects first, so KEDA hands replica control back before it goes
kubectl delete scaledobjects.keda.sh --all -A --ignore-not-found >/dev/null 2>&1 \
  || echo "   (no ScaledObject CRD — KEDA not installed)"
kubectl delete --ignore-not-found \
  -f "https://github.com/kedacore/keda/releases/download/${KEDA_VERSION}/keda-${KEDA_VERSION#v}.yaml" >/dev/null
kubectl delete --ignore-not-found \
  -f "https://github.com/kubernetes-sigs/metrics-server/releases/download/${METRICS_SERVER_VERSION}/components.yaml" >/dev/null
ns=$(kubectl get namespace keda --ignore-not-found -o name)
[ -z "$ns" ] || echo "   namespace keda still terminating; make mode-k8s refuses until it is gone"
echo "KEDA and metrics-server removed"
