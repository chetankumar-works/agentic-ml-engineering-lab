#!/usr/bin/env bash
# Read-only memory sampler for budget measurements (DECISIONS.md ADR-0012).
# Reads cgroup v2 files from the WSL side — no cluster API calls in the
# hot loop, so it is cheap enough for sub-second intervals.
#
#   scripts/mem_sample.sh OUT DURATION_S [INTERVAL_S] [FAST_INTERVAL_S FAST_FOR_S]
#   e.g. scripts/mem_sample.sh /tmp/run.tsv 300 2 0.25 60   # 250 ms for the first minute
#
# Columns: epoch_ms target anon_mib shmem_mib file_mib current_mib full_avg10 max_events
# Targets: vm (MemAvailable in the anon column), node, each amel-* Compose
# container, and each pod in the `amel` namespace (pod:<name>, when the
# node is running; the pod list is refreshed every 5 s).
# Use anon (+shmem) for budgets: `docker stats` and `current` include page cache.
set -euo pipefail
OUT=$1 DURATION=$2 INTERVAL=${3:-2} FAST=${4:-} FAST_FOR=${5:-0}
NODE=amel-control-plane
declare -A CG

cg_of() { local id; id=$(docker inspect -f '{{.Id}}' "$1" 2>/dev/null) && echo "/sys/fs/cgroup/docker/$id"; }
refresh_pods() {
  local base=$1 uid ns name dir
  for k in "${!CG[@]}"; do [[ $k == pod:* ]] && unset "CG[$k]"; done
  while read -r uid name; do
    dir=$(find "$base/kubelet.slice" -maxdepth 3 -type d -name "*pod${uid//-/_}.slice" 2>/dev/null | head -1)
    [ -n "$dir" ] && CG["pod:$name"]=$dir
  done < <(kubectl -n amel get pods -o custom-columns=U:.metadata.uid,N:.metadata.name --no-headers 2>/dev/null || true)
}
sample() {  # target dir
  local d=$2 a s f cur full maxev
  [ -r "$d/memory.stat" ] || return 0
  read -r a s f < <(awk '$1=="anon"{a=$2}$1=="shmem"{s=$2}$1=="file"{f=$2}END{print int(a/1048576), int(s/1048576), int(f/1048576)}' "$d/memory.stat")
  cur=$(( $(cat "$d/memory.current") / 1048576 ))
  full=$(awk '/^full/{split($2,x,"=");print x[2]}' "$d/memory.pressure" 2>/dev/null || echo -)
  maxev=$(awk '$1=="max"{print $2}' "$d/memory.events" 2>/dev/null || echo -)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$now" "$1" "$a" "$s" "$f" "$cur" "$full" "$maxev"
}

for c in $(docker ps --format '{{.Names}}' | grep '^amel-'); do d=$(cg_of "$c") && CG[$c]=$d; done
printf 'epoch_ms\ttarget\tanon_mib\tshmem_mib\tfile_mib\tcurrent_mib\tfull_avg10\tmax_events\n' > "$OUT"
start=$SECONDS last_refresh=-5
while [ $(( SECONDS - start )) -lt "$DURATION" ]; do
  if [ -n "${CG[$NODE]:-}" ] && [ $(( SECONDS - last_refresh )) -ge 5 ]; then
    refresh_pods "${CG[$NODE]}"; last_refresh=$SECONDS
  fi
  t0=${EPOCHREALTIME/./}; now=$(( t0 / 1000 ))   # µs -> ms; `date +%3N` is not portable here
  {
    printf '%s\tvm\t%s\t-\t-\t-\t-\t-\n' "$now" "$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo)"
    for t in "${!CG[@]}"; do sample "$t" "${CG[$t]}"; done
  } >> "$OUT"
  # sleep only what is left of the interval (a sweep itself takes ~90 ms)
  step=$INTERVAL; [ -n "$FAST" ] && [ $(( SECONDS - start )) -lt "$FAST_FOR" ] && step=$FAST
  rem=$(( $(awk -v s="$step" 'BEGIN{printf "%d", s*1000000}') - (${EPOCHREALTIME/./} - t0) ))
  [ "$rem" -gt 0 ] && sleep "$(awk -v r="$rem" 'BEGIN{printf "%.6f", r/1000000}')"
done
