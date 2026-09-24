#!/usr/bin/env bash
# metrics-server + KEDA on the kind cluster for Milestone 10, with explicit
# memory limits (DECISIONS.md ADR-0012). Scale mode only: k8s mode's 3g
# budget does not include them. `scripts/autoscaling_down.sh` removes both.
#
# Upstream defaults this overrides: KEDA's three Deployments ship 1000Mi
# limits each (2.9 GiB together, about the whole k8s-mode node);
# metrics-server ships no limit. Each gets 256Mi here, and its measured
# anon + hot file pages must stay <= 75% of that.
set -euo pipefail
cd "$(dirname "$0")/.."
METRICS_SERVER_VERSION="${METRICS_SERVER_VERSION:-v0.9.0}"
KEDA_VERSION="${KEDA_VERSION:-v2.21.0}"
LIMIT="${AUTOSCALING_MEMORY_LIMIT:-256Mi}"

echo "== 1. mode check"
./scripts/mode.sh check scale

echo "== 2. metrics-server $METRICS_SERVER_VERSION"
kubectl apply -f "https://github.com/kubernetes-sigs/metrics-server/releases/download/${METRICS_SERVER_VERSION}/components.yaml" >/dev/null
# kind's kubelets serve self-signed certs. json-patch "add" is not
# idempotent, so patch only when the flag is absent (re-runs).
args=$(kubectl -n kube-system get deployment metrics-server -o jsonpath='{.spec.template.spec.containers[0].args}')
if [[ $args != *--kubelet-insecure-tls* ]]; then
  kubectl -n kube-system patch deployment metrics-server --type json -p \
    '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' >/dev/null
fi
kubectl -n kube-system set resources deployment/metrics-server --limits=memory="$LIMIT" >/dev/null
kubectl -n kube-system rollout status deployment/metrics-server --timeout=180s >/dev/null
echo "   ready"

echo "== 3. KEDA $KEDA_VERSION"
# server-side: the CRDs exceed the client-side last-applied annotation size
kubectl apply --server-side --force-conflicts \
  -f "https://github.com/kedacore/keda/releases/download/${KEDA_VERSION}/keda-${KEDA_VERSION#v}.yaml" >/dev/null
for d in keda-operator keda-metrics-apiserver keda-admission; do
  kubectl -n keda set resources deployment/"$d" --limits=memory="$LIMIT" >/dev/null
done
for d in keda-operator keda-metrics-apiserver keda-admission; do
  kubectl -n keda rollout status deployment/"$d" --timeout=300s >/dev/null && echo "   $d ready"
done

echo "== 4. APIs"
kubectl wait --for=condition=Available --timeout=180s \
  apiservice/v1beta1.metrics.k8s.io apiservice/v1beta1.external.metrics.k8s.io >/dev/null
for i in $(seq 1 24); do kubectl top nodes >/dev/null 2>&1 && break; sleep 5; done
kubectl top nodes
