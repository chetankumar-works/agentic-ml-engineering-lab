#!/usr/bin/env python3
"""Failure-engineering regression: the source simulator's probes must
tell the truth when Kafka stops accepting writes.

Reproduces the Milestone 3 incident (RUNBOOKS.md "source_simulator
producer wedge"): the broker stalled, librdkafka timed out every
in-flight message, the publishing loop died — and /health kept
returning 200 for an hour. Milestone 8's Kubernetes probes and
Milestone 10's KEDA backlog bursts both depend on these endpoints
being honest, so this is a test, not a memory.

Scenario (against a live `make up` stack):
  1. precondition: /ready is 200 and rows_read is advancing
  2. SIGSTOP the broker (`docker compose pause kafka`) — connections hang,
     nothing is acked, every message times out after
     KAFKA_MESSAGE_TIMEOUT_MS (30 s by default)
  3. expect /ready -> 503 naming the Kafka error, within timeout + slack
  4. resume the broker (`docker compose unpause kafka`)
  5. expect recovery: /ready -> 200 and rows_read advancing again.
     If librdkafka declared the idempotent producer *fatally* broken
     instead, /health must be 503 (so a liveness probe would restart it);
     the script then restarts the container itself and expects recovery.

Usage: `make failure-simulator-wedge`, or
    uv run python scripts/failure_engineering/simulator_producer_wedge.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

SIMULATOR_URL = os.environ.get("SIMULATOR_URL", "http://localhost:8001")
COMPOSE = ["docker", "compose", "-f", "infra/docker-compose.yml"]
MESSAGE_TIMEOUT_S = int(os.environ.get("KAFKA_MESSAGE_TIMEOUT_MS", "30000")) / 1000
DETECT_DEADLINE_S = MESSAGE_TIMEOUT_S + 60
RECOVER_DEADLINE_S = 180


def probe(path: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(f"{SIMULATOR_URL}{path}", timeout=5) as r:  # noqa: S310
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def status() -> dict:
    return json.loads(probe("/status")[1])


def rows_read() -> int:
    return int(status()["counters"]["rows_read"])


def compose(*args: str) -> None:
    subprocess.run([*COMPOSE, *args], check=True, capture_output=True)


def wait_for(description: str, predicate, deadline_s: float) -> float:  # noqa: ANN001
    started = time.monotonic()
    while time.monotonic() - started < deadline_s:
        if predicate():
            return time.monotonic() - started
        time.sleep(2)
    raise TimeoutError(f"{description} did not happen within {deadline_s:.0f}s")


def expect_advancing(label: str) -> None:
    before = rows_read()
    wait_for(f"{label}: rows_read advancing", lambda: rows_read() > before, 30)
    print(f"  {label}: rows_read advancing ({before} -> {rows_read()})")


def main() -> int:
    print("=== 1. precondition ===")
    code, reason = probe("/ready")
    if code != 200:
        print(f"FAIL: /ready is {code} ({reason}) before the experiment — fix the stack first")
        return 1
    expect_advancing("precondition")
    failures_before = status()["producer_health"]["delivery_failures"]

    kafka_paused = False
    try:
        print("=== 2. SIGSTOP the broker ===")
        compose("pause", "kafka")
        kafka_paused = True

        print(f"=== 3. expect /ready -> 503 within {DETECT_DEADLINE_S:.0f}s ===")
        took = wait_for("/ready -> 503", lambda: probe("/ready")[0] == 503, DETECT_DEADLINE_S)
        code, reason = probe("/ready")
        print(f"  /ready {code} after {took:.0f}s: {reason!r}")
        if "kafka" not in reason.lower():
            print("FAIL: readiness failed but the reason does not name Kafka")
            return 1
        health_code, health_reason = probe("/health")
        s = status()
        print(f"  /health {health_code}: {health_reason!r}")
        print(
            f"  delivery_failures: {failures_before} -> {s['producer_health']['delivery_failures']}"
        )
        if s["producer_health"]["delivery_failures"] <= failures_before:
            print("FAIL: /ready is 503 but no delivery failures were recorded")
            return 1
        fatal = health_code == 503

        print("=== 4. resume the broker ===")
        compose("unpause", "kafka")
        kafka_paused = False

        print("=== 5. expect recovery ===")
        if fatal:
            print(f"  producer declared FATAL ({health_reason!r})")
            print("  a liveness probe would restart it; doing so")
            compose("restart", "source-simulator")
        took = wait_for("/ready -> 200", lambda: probe("/ready")[0] == 200, RECOVER_DEADLINE_S)
        how = "after restart" if fatal else "self-recovered, no restart"
        print(f"  /ready 200 after {took:.0f}s ({how})")
        expect_advancing("post-recovery")
    finally:
        if kafka_paused:
            compose("unpause", "kafka")

    print("\nsimulator producer wedge: PASSED — probes reported the outage and recovery truthfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
