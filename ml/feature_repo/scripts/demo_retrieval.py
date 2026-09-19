#!/usr/bin/env python3
"""Milestone 3 acceptance demo: proves both the historical (offline,
point-in-time-correct) and online (materialized, low-latency) feature
retrieval paths work against real curated data — not two isolated API
calls, but the actual pattern a training job and an inference service
would each use.

Usage: `make feast-demo` (after `make up` and `feast apply` have run), or
directly:
    uv run --package amel-feature-repo python ml/feature_repo/scripts/demo_retrieval.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from amel_common.schemas import HIGGS_FEATURE_NAMES
from amel_db.session import session_scope
from feast import FeatureStore
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent))
from materialize import materialize_chunked, resume_point  # noqa: E402

REPO_PATH = str(Path(__file__).resolve().parent.parent)
FEATURE_REFS = [f"higgs_features:{name}" for name in HIGGS_FEATURE_NAMES]
SAMPLE_SIZE = 20


def _sample_entity_df() -> pd.DataFrame:
    """A real entity dataframe built from curated.higgs_labels — exactly
    what a training job would build: "for these entities, at these label
    timestamps, what were the features?" Feast's point-in-time join then
    finds the correct feature value for each (entity, timestamp) pair.
    """
    with session_scope() as session:
        rows = (
            session.execute(
                text(
                    "SELECT entity_id, label_timestamp AS event_timestamp, target "
                    "FROM curated.higgs_labels ORDER BY label_timestamp DESC LIMIT :n"
                ),
                {"n": SAMPLE_SIZE},
            )
            .mappings()
            .all()
        )
    if not rows:
        raise RuntimeError(
            "curated.higgs_labels is empty — run the Milestone 1/2 pipeline "
            "(make up, then trigger higgs_pipeline) before this demo."
        )
    return pd.DataFrame(rows)


def demo_historical_retrieval(store: FeatureStore) -> pd.DataFrame:
    entity_df = _sample_entity_df()
    print(f"entity dataframe: {len(entity_df)} rows (from curated.higgs_labels)")

    result = store.get_historical_features(entity_df=entity_df, features=FEATURE_REFS).to_df()
    missing = int(result[list(HIGGS_FEATURE_NAMES)].isna().any(axis=1).sum())
    print(f"historical retrieval: {len(result)} rows, {missing} with any missing feature")
    if len(result) > 0 and missing == len(result):
        raise RuntimeError(
            "every row came back with no features — point-in-time join found nothing"
        )
    preview_cols = ["entity_id", "event_timestamp", "target", "lepton_pt", "m_bb"]
    print(result[preview_cols].head(5).to_string(index=False))

    # Point-in-time correctness, the negative case: ask for the same
    # entities as-of a moment *before* their feature events existed. A
    # correct join must return nothing — a naive "latest value per entity"
    # join would leak future data into training.
    stale_df = entity_df.assign(event_timestamp=entity_df["event_timestamp"] - pd.Timedelta(days=1))
    stale = store.get_historical_features(entity_df=stale_df, features=FEATURE_REFS).to_df()
    leaked = int(stale[list(HIGGS_FEATURE_NAMES)].notna().any(axis=1).sum())
    print(f"as-of one day earlier: {len(stale)} rows, {leaked} with leaked (future) features")
    if leaked:
        raise RuntimeError("point-in-time join returned features from the future")
    return result


def demo_online_retrieval(store: FeatureStore) -> dict[str, list]:
    # Chunked (see materialize.py) rather than store.materialize_incremental():
    # the latter loads the whole un-materialized window at once and was
    # OOM-killed at 12 GB over the full curated table.
    resume = resume_point(store)
    print(f"materializing offline -> online (Redis), resuming from {resume or 'scratch'}...")
    n_windows = materialize_chunked(store, end=datetime.now(UTC))
    print(f"materialized {n_windows} window(s)")

    with session_scope() as session:
        sample_ids = [
            row[0]
            for row in session.execute(
                text("SELECT entity_id FROM curated.higgs_features ORDER BY loaded_at DESC LIMIT 5")
            )
        ]
    if not sample_ids:
        raise RuntimeError("curated.higgs_features is empty — nothing to materialize")

    online = store.get_online_features(
        features=FEATURE_REFS,
        entity_rows=[{"entity_id": eid} for eid in sample_ids],
    ).to_dict()
    n_with_values = sum(1 for v in online["lepton_pt"] if v is not None)
    print(
        f"online retrieval: {len(sample_ids)} entities requested, {n_with_values} returned a value"
    )
    if n_with_values == 0:
        raise RuntimeError("materialize ran but the online store returned no values for any entity")

    # Online/offline consistency: the value an inference service reads
    # from Redis must be the value a training job reads from Postgres for
    # the same entity — that is the train/serve-skew guarantee. Float32
    # in Feast vs float8 in the view, hence the tolerance.
    with session_scope() as session:
        offline: dict[str, float] = {
            row[0]: row[1]
            for row in session.execute(
                text(
                    "SELECT entity_id, lepton_pt FROM curated.higgs_features_flat "
                    "WHERE entity_id = ANY(:ids)"
                ),
                {"ids": sample_ids},
            ).all()
        }
    mismatched = [
        eid
        for eid, v in zip(online["entity_id"], online["lepton_pt"], strict=True)
        if v is None or abs(v - offline[eid]) > 1e-6
    ]
    print(f"online == offline for lepton_pt: {len(sample_ids) - len(mismatched)}/{len(sample_ids)}")
    if mismatched:
        raise RuntimeError(f"online/offline mismatch for {mismatched}")
    return online


def main() -> int:
    store = FeatureStore(repo_path=REPO_PATH)
    fv = store.get_feature_view("higgs_features")
    # Feature versioning: the registry is the source of truth for *which*
    # definition of "higgs_features" a training run or online lookup used —
    # the tag below is what a training job records alongside its model.
    print(f"feature view: {fv.name} version={fv.tags.get('version')} features={len(fv.features)}")

    print("=== historical (offline, point-in-time) retrieval ===")
    try:
        demo_historical_retrieval(store)
    except Exception as exc:  # noqa: BLE001 — top-level demo script, report and exit non-zero
        print(f"FAIL: historical retrieval: {exc}", file=sys.stderr)
        return 1

    print("\n=== online (materialized, low-latency) retrieval ===")
    try:
        demo_online_retrieval(store)
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: online retrieval: {exc}", file=sys.stderr)
        return 1

    print("\nMilestone 3 demo PASSED: historical and online retrieval both work.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
