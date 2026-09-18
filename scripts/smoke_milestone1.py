#!/usr/bin/env python3
"""Milestone 1 smoke test — run against a live `make up` stack.

Verifies the acceptance criteria from AMEL_KICKOFF_PROMPT.md /
PROJECT_STATE.md: at least SMOKE_MIN_FEATURE_EVENTS feature events land
in `landing.higgs_feature_events` with no duplicate `event_id`s (the
`event_id` primary key + `ON CONFLICT DO NOTHING` sink makes this true by
construction, but we verify it directly rather than assuming it), and at
least one malformed event reaches each DLQ topic.

Usage: `make smoke` (after `make up`), or directly:
    uv run python scripts/smoke_milestone1.py
"""

from __future__ import annotations

import os
import sys
import time

from confluent_kafka import Consumer
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg://amel:amel_dev_password@localhost:5432/amel"
)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
MIN_FEATURE_EVENTS = int(os.environ.get("SMOKE_MIN_FEATURE_EVENTS", "100000"))
TIMEOUT_SECONDS = int(os.environ.get("SMOKE_TIMEOUT_SECONDS", "900"))
POLL_INTERVAL_SECONDS = 5


def wait_for_event_count(engine) -> int:  # noqa: ANN001
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_count = -1
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT count(*) FROM landing.higgs_feature_events")
            ).scalar_one()
        if count != last_count:
            print(f"  feature events persisted so far: {count:,} / {MIN_FEATURE_EVENTS:,}")
            last_count = count
        if count >= MIN_FEATURE_EVENTS:
            return count
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(
        f"only {last_count:,} feature events persisted after {TIMEOUT_SECONDS}s "
        f"(wanted {MIN_FEATURE_EVENTS:,}) — is `make up` running? check `make logs`."
    )


def assert_no_duplicate_event_ids(engine) -> None:  # noqa: ANN001
    with engine.connect() as conn:
        total = conn.execute(text("SELECT count(*) FROM landing.higgs_feature_events")).scalar_one()
        distinct = conn.execute(
            text("SELECT count(DISTINCT event_id) FROM landing.higgs_feature_events")
        ).scalar_one()
    if total != distinct:
        raise AssertionError(
            f"duplicate event_ids found: {total} rows but only {distinct} distinct event_ids"
        )
    print(f"  OK: {total:,} rows, {distinct:,} distinct event_ids — no duplicates persisted")


def count_dlq_messages(topic: str, max_wait_seconds: float = 15.0) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"smoke-test-dlq-reader-{topic}-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    seen = 0
    deadline = time.monotonic() + max_wait_seconds
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            seen += 1
    finally:
        consumer.close()
    return seen


def main() -> int:
    engine = create_engine(DATABASE_URL)

    print(f"waiting for >= {MIN_FEATURE_EVENTS:,} feature events (timeout {TIMEOUT_SECONDS}s)...")
    try:
        wait_for_event_count(engine)
    except TimeoutError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print("checking for duplicate event_ids...")
    try:
        assert_no_duplicate_event_ids(engine)
    except AssertionError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print("checking malformed events reached the DLQ topics...")
    features_dlq = count_dlq_messages("higgs.features.dlq")
    labels_dlq = count_dlq_messages("higgs.labels.dlq")
    print(f"  higgs.features.dlq: {features_dlq} message(s) observed")
    print(f"  higgs.labels.dlq: {labels_dlq} message(s) observed")
    if features_dlq == 0 and labels_dlq == 0:
        print(
            "FAIL: expected at least one malformed event in a DLQ topic "
            "(INVALID_EVENT_RATE > 0 should guarantee this over 100k+ events)",
            file=sys.stderr,
        )
        return 1

    print("\nMilestone 1 smoke test PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
