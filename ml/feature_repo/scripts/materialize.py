#!/usr/bin/env python3
"""Chunked offline -> online materialization for the HIGGS feature view.

Why this exists instead of plain `feast materialize-incremental`: Feast's
Postgres offline store materializes a time window by loading *every* row
in it into memory and converting each one into protobufs (~14 KB/row
observed). Over the ~885k-row curated table that reached 12 GB RSS and
was OOM-killed — and is almost certainly what took WSL2 down during the
first Milestone 3 attempt (see RUNBOOKS.md). Peak memory must be bounded
by *chunk size*, not table size, so this walks the source in windows of
at most `chunk_rows` rows and calls `store.materialize(start, end)` per
window. Feast records each window in the registry, so a re-run (or the
Airflow `update_feature_store` task) resumes from the last materialized
end rather than starting over.

Usage:
    python scripts/materialize.py            # resume from registry, up to now
    python scripts/materialize.py --chunk-rows 10000 --end 2026-09-19T00:00:00
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from amel_db.session import session_scope
from feast import FeatureStore
from sqlalchemy import text

REPO_PATH = str(Path(__file__).resolve().parent.parent)
FEATURE_VIEW = "higgs_features"
SOURCE_TABLE = "curated.higgs_features_flat"
TIMESTAMP_COLUMN = "event_timestamp"
DEFAULT_CHUNK_ROWS = 25_000
# Feast's postgres offline store uses `BETWEEN start AND end` (inclusive
# both ends), so a boundary row belongs to two adjacent windows. Harmless
# — the online store just receives the same latest value twice.


def windows(
    start: datetime, end: datetime, boundaries: Sequence[datetime]
) -> list[tuple[datetime, datetime]]:
    """Split [start, end] at each boundary, dropping empty/degenerate windows.
    Pure function so the chunking logic is unit-testable without Feast or Postgres."""
    edges = [start, *[b for b in boundaries if start < b < end], end]
    return [(lo, hi) for lo, hi in zip(edges, edges[1:], strict=False) if lo < hi]


def _source_bounds() -> tuple[datetime | None, datetime | None]:
    with session_scope() as session:
        row = session.execute(
            text(f"SELECT min({TIMESTAMP_COLUMN}), max({TIMESTAMP_COLUMN}) FROM {SOURCE_TABLE}")
        ).one()
    return row[0], row[1]


def _chunk_boundaries(start: datetime, end: datetime, chunk_rows: int) -> list[datetime]:
    """Every `chunk_rows`-th event_timestamp in [start, end] — computed in
    SQL so the window sizes track actual data density (the simulator's
    rate varies) rather than a fixed wall-clock width."""
    with session_scope() as session:
        rows = session.execute(
            text(
                f"""
                SELECT ts FROM (
                    SELECT {TIMESTAMP_COLUMN} AS ts,
                           row_number() OVER (ORDER BY {TIMESTAMP_COLUMN}) AS rn
                    FROM {SOURCE_TABLE}
                    WHERE {TIMESTAMP_COLUMN} >= :start AND {TIMESTAMP_COLUMN} <= :end
                ) numbered
                WHERE rn % :chunk_rows = 0
                ORDER BY ts
                """
            ),
            {"start": start, "end": end, "chunk_rows": chunk_rows},
        ).all()
    return [r[0] for r in rows]


def _as_utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def resume_point(store: FeatureStore) -> datetime | None:
    """Where the registry says materialization last stopped, or None if never run."""
    intervals = store.get_feature_view(FEATURE_VIEW).materialization_intervals
    return max((_as_utc(hi) for _, hi in intervals), default=None)


def plan(
    store: FeatureStore, end: datetime, chunk_rows: int
) -> Iterator[tuple[datetime, datetime]]:
    src_min, src_max = _source_bounds()
    if src_min is None or src_max is None:
        return iter(())
    start = resume_point(store) or _as_utc(src_min)
    end = min(end, _as_utc(src_max))  # nothing after the newest source row
    if start >= end:
        return iter(())
    return iter(
        windows(start, end, [_as_utc(b) for b in _chunk_boundaries(start, end, chunk_rows)])
    )


def materialize_chunked(
    store: FeatureStore, end: datetime, chunk_rows: int = DEFAULT_CHUNK_ROWS
) -> int:
    """Materialize everything not yet materialized up to `end`; returns windows processed."""
    n = 0
    for lo, hi in plan(store, end, chunk_rows):
        print(f"  materialize [{lo.isoformat()} -> {hi.isoformat()}]", flush=True)
        store.materialize(start_date=lo, end_date=hi, feature_views=[FEATURE_VIEW])
        n += 1
    return n


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS)
    parser.add_argument(
        "--end", type=datetime.fromisoformat, default=None, help="ISO timestamp (default: now, UTC)"
    )
    args = parser.parse_args(argv)
    end = _as_utc(args.end) if args.end else datetime.now(UTC)

    store = FeatureStore(repo_path=REPO_PATH)
    resume = resume_point(store)
    print(f"resume point from registry: {resume.isoformat() if resume else 'none (first run)'}")
    started = datetime.now(UTC)
    n = materialize_chunked(store, end=end, chunk_rows=args.chunk_rows)
    print(f"done: {n} window(s) in {(datetime.now(UTC) - started) // timedelta(seconds=1)}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
