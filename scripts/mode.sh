#!/usr/bin/env bash
# Compose/kind modes (DECISIONS.md ADR-0012). The full Compose stack and
# the kind cluster never run together; each cluster mode names the
# Compose services it keeps, and the node's memory cap identifies the mode.
#
#   mode      node                      Compose runs
#   compose   stopped or absent         full stack
#   k8s       running, cap 3g, no KFP   everything except the four moved services
#   kfp       running, cap 6g           postgres, minio, mlflow only
#
# Usage: scripts/mode.sh status | check MODE | compose | k8s | kfp
#
# A node that may still hold KFP is only ever started at the KFP cap with
# Compose trimmed to the kfp set, so the worst case at every step is the
# kfp budget. KFP inside a node capped at 3g livelocks it (MISTAKES.md, M9).
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f infra/docker-compose.yml"
NODE=amel-control-plane
K8S_CAP="${KIND_NODE_MEMORY:-3g}"
KFP_CAP="${KIND_NODE_MEMORY_KFP:-6g}"
MOVED="inference-api feast-server source-simulator stream-ingestor"
KFP_KEEP="minio mlflow postgres"
exec 3>&2   # the real stderr, for errors that must survive `check 2>/dev/null`

bytes() { numfmt --from=iec "${1^^}"; }
words() { tr ' ' '\n' | sed '/^$/d' | sort -u; }
running() { $COMPOSE ps --status running --services | words; }
all_services() { $COMPOSE config --services | words; }
allowed() {
  case "$1" in
    compose) all_services ;;
    k8s) comm -23 <(all_services) <(echo "$MOVED" | words) ;;
    kfp) echo "$KFP_KEEP" | words ;;
  esac
}
cluster_exists() { kind get clusters 2>/dev/null | grep -qx amel; }
node_running() { [ "$(docker inspect -f '{{.State.Running}}' "$NODE" 2>/dev/null)" = true ]; }
node_cap() { docker inspect -f '{{.HostConfig.Memory}}' "$NODE"; }
# Exits on an unreachable API: callers run in `if` context, where set -e
# is off and an empty answer would read as "not installed".
kfp_installed() {
  local ns
  ns=$(kubectl get namespace kubeflow --ignore-not-found -o name) \
    || { echo "cannot reach the kind API to check for KFP" >&3; exit 1; }
  [ -n "$ns" ]
}
wait_api() {
  local deadline=$(( SECONDS + 180 ))
  until kubectl get --raw /readyz >/dev/null 2>&1; do
    [ "$SECONDS" -ge "$deadline" ] && { echo "kind API not ready after 180s" >&2; return 1; }
    sleep 3
  done
}
set_cap() { docker update --memory "$1" --memory-swap "$1" "$NODE" >/dev/null; echo "   node cap $1"; }

# Prints violations of MODE to stderr; exit status 0 only if none.
check() {
  local mode=$1 bad=0 extra
  extra=$(comm -13 <(allowed "$mode") <(running) | tr '\n' ' ')
  [ -n "$extra" ] && { echo "   not allowed in $mode mode: $extra" >&2; bad=1; }
  if [ "$mode" = compose ]; then
    node_running && { echo "   kind node is running" >&2; bad=1; }
  else
    if ! node_running; then
      echo "   kind node is not running" >&2; bad=1
    else
      local want; want=$(bytes "$([ "$mode" = kfp ] && echo "$KFP_CAP" || echo "$K8S_CAP")")
      [ "$(node_cap)" = "$want" ] || { echo "   node cap $(node_cap) bytes, $mode mode needs $want" >&2; bad=1; }
      if [ "$mode" = k8s ] && kfp_installed; then
        echo "   KFP is installed; k8s mode's cap would livelock the node" >&2; bad=1
      fi
    fi
  fi
  return "$bad"
}

status() {
  if ! cluster_exists; then echo "node: absent"
  elif node_running; then echo "node: running, cap $(( $(node_cap) / 1073741824 ))g"
  else echo "node: stopped"; fi
  echo "compose: $(running | tr '\n' ' ')"
  local m
  for m in compose k8s kfp; do
    if check "$m" 2>/dev/null; then echo "mode: $m"; return 0; fi
  done
  echo "mode: NONE — the running set matches no mode:" >&2
  for m in compose k8s kfp; do echo " $m:" >&2; check "$m" || true; done
  return 1
}

# Stop every running Compose service outside MODE's list, and verify.
trim_compose() {
  local extra
  extra=$(comm -13 <(allowed "$1") <(running) | tr '\n' ' ')
  if [ -n "$extra" ]; then
    echo "   stopping: $extra"
    # shellcheck disable=SC2086
    $COMPOSE stop $extra >/dev/null
  fi
  extra=$(comm -13 <(allowed "$1") <(running) | tr '\n' ' ')
  [ -z "$extra" ] || { echo "still running after stop: $extra" >&2; return 1; }
}

higgs_start_index() {
  local maxid
  maxid=$(docker exec amel-postgres-1 psql -U amel -d amel -tAc \
    "SELECT coalesce(max(entity_id),'higgs-000000000') FROM landing.higgs_feature_events")
  echo $(( 10#${maxid#higgs-} + 1 ))
}

# Start a stopped node inside the kfp envelope (Compose trimmed, 6g cap),
# so an unknown KFP install cannot livelock it or crowd the VM.
start_node_in_kfp_envelope() {
  echo "== trimming Compose to the kfp set before starting the node"
  trim_compose kfp
  set_cap "$KFP_CAP"
  node_running || docker start "$NODE" >/dev/null
  wait_api
}

to_compose() {
  if node_running; then echo "== stopping the kind node"; docker stop -t 60 "$NODE" >/dev/null; fi
  echo "== full Compose stack"
  $COMPOSE up -d --wait postgres >/dev/null
  HIGGS_START_INDEX=$(higgs_start_index) $COMPOSE up -d >/dev/null
}

to_kfp() {
  cluster_exists || { echo "no kind cluster: run make mode-k8s first (it creates one)" >&2; exit 1; }
  start_node_in_kfp_envelope
  $COMPOSE up -d --wait $KFP_KEEP >/dev/null
  if [ -n "$(kubectl get namespace amel --ignore-not-found -o name)" ]; then
    kubectl -n amel scale deployment --all --replicas=0 >/dev/null
    echo "   amel Deployments scaled to 0"
  fi
}

to_k8s() {
  if ! cluster_exists; then
    echo "== no cluster: creating one next to Compose minus the moved services"
    trim_compose k8s
    exec ./scripts/k8s_up.sh
  fi
  if ! node_running || [ "$(node_cap)" != "$(bytes "$K8S_CAP")" ]; then
    start_node_in_kfp_envelope
    if kfp_installed; then
      echo "== KFP still installed: removing it first"
      ./scripts/kfp_down.sh
    else
      set_cap "$K8S_CAP"
    fi
  elif kfp_installed; then
    echo "KFP is installed in a node capped at $K8S_CAP; run make kfp-down" >&2; exit 1
  fi
  trim_compose k8s
  echo "== Compose minus the moved services"
  # shellcheck disable=SC2046
  FEAST_SERVER_URL=http://amel-control-plane:30566 $COMPOSE up -d $(allowed k8s) >/dev/null
  ./scripts/k8s_up.sh
}

case "${1:-status}" in
  status) status ;;
  check)
    [ -n "${2:-}" ] || { echo "usage: $0 check compose|k8s|kfp" >&2; exit 2; }
    check "$2" || { echo "not in $2 mode (make mode-$2)" >&2; exit 1; } ;;
  compose) to_compose; status ;;
  k8s) to_k8s; status ;;
  kfp) to_kfp; status ;;
  *) echo "usage: $0 status|check MODE|compose|k8s|kfp" >&2; exit 2 ;;
esac
