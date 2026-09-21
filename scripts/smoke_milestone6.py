#!/usr/bin/env python3
"""Milestone 6 smoke test — one prediction, traced end to end.

Against a live `make up` stack: issue a /predict/entity request, then
prove the SAME trace id (a) came back in the response, (b) is a trace in
Tempo spanning inference_api AND feast_server, (c) is on the log line
in Loki, (d) is on the ml.predictions row; and that Prometheus has all
scrape targets up plus span-derived RED metrics.

Usage: `make smoke-tracing`, or
    uv run python scripts/smoke_milestone6.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request

INFERENCE_URL = os.environ.get("INFERENCE_URL", "http://localhost:8003")
TEMPO_URL = os.environ.get("TEMPO_URL", "http://localhost:3200")
LOKI_URL = os.environ.get("LOKI_URL", "http://localhost:3100")
PROM_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg://amel:amel_dev_password@localhost:5432/amel"
)


def get(url: str) -> tuple[int, dict, bytes]:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15) as r:  # noqa: S310
        return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()


def post(url: str, body: dict | None = None) -> tuple[int, dict, bytes]:
    data = json.dumps(body).encode() if body is not None else b""
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as r:  # noqa: S310
        return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()


def wait_for(description: str, fn, deadline_s: float = 45.0):  # noqa: ANN001, ANN201
    started = time.monotonic()
    last = None
    while time.monotonic() - started < deadline_s:
        try:
            last = fn()
            if last:
                return last
        except Exception as exc:  # noqa: BLE001 — retried
            last = exc
        time.sleep(2)
    raise TimeoutError(f"{description}: not observed within {deadline_s:.0f}s (last: {last!r})")


def main() -> int:
    from sqlalchemy import create_engine, text

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        recent = "SELECT entity_id FROM curated.higgs_features ORDER BY loaded_at DESC"
        entity = conn.execute(text(recent + " LIMIT 1 OFFSET 500")).scalar_one()

    print("=== 1. prediction ===")
    status, headers, body = post(f"{INFERENCE_URL}/predict/entity/{entity}")
    resp = json.loads(body)
    trace_id = resp["trace_id"]
    assert status == 200 and headers.get("x-trace-id") == trace_id
    version = resp["model"]["version"]
    print(f"  entity {entity} -> prediction {resp['prediction']} (model v{version})")
    print(f"  trace {trace_id}")

    print("=== 2. Tempo: the trace spans two services ===")

    def tempo_trace():  # noqa: ANN202
        s, _, b = get(f"{TEMPO_URL}/api/traces/{trace_id}")
        d = json.loads(b)
        services = sorted(
            {
                a["value"]["stringValue"]
                for batch in d.get("batches", [])
                for a in batch["resource"]["attributes"]
                if a["key"] == "service.name"
            }
        )
        spans = sum(
            len(ss["spans"]) for batch in d.get("batches", []) for ss in batch["scopeSpans"]
        )
        return (services, spans) if {"inference_api", "feast_server"} <= set(services) else None

    services, spans = wait_for("trace in Tempo with both services", tempo_trace)
    print(f"  {spans} spans across {services}")

    print("=== 3. Loki: a log line carries the trace id ===")

    def loki_line():  # noqa: ANN202
        q = urllib.parse.quote(f'{{service_name="inference_api"}} |= "{trace_id}"')
        _, _, b = get(f"{LOKI_URL}/loki/api/v1/query_range?query={q}&limit=5")
        lines = [v[1] for r in json.loads(b)["data"]["result"] for v in r["values"]]
        return lines or None

    lines = wait_for("log line in Loki", loki_line)
    print(f"  {len(lines)} line(s), e.g. {lines[0][:120]}...")

    print("=== 4. Postgres: the ml.predictions row carries it ===")
    with engine.connect() as conn:
        n = conn.execute(
            text("SELECT count(*) FROM ml.predictions WHERE trace_id = :t"), {"t": trace_id}
        ).scalar_one()
    assert n == 1, f"expected 1 row, got {n}"
    print("  1 row")

    print("=== 5. Prometheus: targets up + span-derived RED metrics ===")
    _, _, b = get(f"{PROM_URL}/api/v1/targets")
    # First-party jobs have a Compose target and a kind NodePort target
    # (Milestone 8); exactly one of them answers depending on where the
    # service runs, so the rule is "at least one up per job".
    targets: dict[str, list[str]] = {}
    for t in json.loads(b)["data"]["activeTargets"]:
        targets.setdefault(t["labels"]["job"], []).append(t["health"])
    down = [j for j, hs in targets.items() if "up" not in hs]
    assert not down, f"jobs with no target up: {down}"
    q = urllib.parse.quote("sum by (service_name) (traces_span_metrics_calls_total)")

    def red_metrics():  # noqa: ANN202
        # spanmetrics flush every 15 s and Prometheus scrapes every 15 s:
        # right after the first-ever prediction they can lag ~30 s.
        _, _, b = get(f"{PROM_URL}/api/v1/query?query={q}")
        svcs = sorted(r["metric"]["service_name"] for r in json.loads(b)["data"]["result"])
        return svcs if {"inference_api", "feast_server"} <= set(svcs) else None

    svcs = wait_for("span-derived RED metrics for inference_api + feast_server", red_metrics, 90)
    print(f"  {len(targets)} targets up; RED metrics for {svcs}")

    print("\nMilestone 6 smoke test PASSED: one trace id links response, Tempo, Loki and Postgres.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
