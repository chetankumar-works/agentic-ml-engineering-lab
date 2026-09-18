"""Pure task logic for the `higgs_pipeline` DAG.

Deliberately independent of Airflow (no `airflow.*` imports) so this
module is unit-testable in the ordinary uv workspace venv — Airflow
itself lives in a separate, non-workspace image (see
`infra/airflow/Dockerfile` and `DECISIONS.md` ADR-0002). Airflow-specific
concerns (the `@task` decorators, XCom, `DagRun` context, retries) live
in the thin `higgs_pipeline.py` next to this file.

See LEARNING_LOG.md's Milestone 2 entry for the full data-flow and
idempotency argument this module implements.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from amel_common.schemas import HIGGS_FEATURE_NAMES
from amel_db.models import (
    CuratedHiggsFeature,
    CuratedHiggsLabel,
    CuratedTrainingRecord,
    HiggsFeatureEvent,
    HiggsLabelEvent,
)
from amel_db.session import session_scope
from amel_lake.validation import ValidationReport, features_schema, labels_schema, validate
from amel_lake.watermark import advance_watermark, get_watermark
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

FEATURES_PIPELINE = "higgs_features"
LABELS_PIPELINE = "higgs_labels"
DEFAULT_MAX_INVALID_FRACTION = 0.05

# Postgres hard-caps a single query at 65535 bind parameters
# (`psycopg.OperationalError: number of parameters must be between 0 and
# 65535`) — a bulk upsert of one row per parameter-column blows past that
# once a watermark window has accumulated enough backlog (discovered
# during the Milestone 2 acceptance run against ~48k real rows; see
# LEARNING_LOG.md). Batching keeps every INSERT well under the limit
# regardless of table width.
UPSERT_BATCH_SIZE = 2000


def _chunked(rows: list[dict], size: int) -> list[list[dict]]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]

FEATURE_COLUMNS = [
    "event_id",
    "entity_id",
    "event_timestamp",
    "schema_version",
    *HIGGS_FEATURE_NAMES,
]
LABEL_COLUMNS = ["event_id", "entity_id", "target", "label_timestamp", "schema_version"]


@dataclass(frozen=True)
class WatermarkBounds:
    features_watermark: datetime
    labels_watermark: datetime
    upper_bound: datetime


def determine_high_watermark(upper_bound: datetime) -> WatermarkBounds:
    return WatermarkBounds(
        features_watermark=get_watermark(FEATURES_PIPELINE),
        labels_watermark=get_watermark(LABELS_PIPELINE),
        upper_bound=upper_bound,
    )


def extract_features(features_watermark: datetime, upper_bound: datetime) -> pd.DataFrame:
    stmt = (
        select(HiggsFeatureEvent)
        .where(HiggsFeatureEvent.ingested_at > features_watermark)
        .where(HiggsFeatureEvent.ingested_at <= upper_bound)
        .order_by(HiggsFeatureEvent.ingested_at)
    )
    with session_scope() as session:
        rows = session.execute(stmt).scalars().all()
        records = [
            {
                "event_id": r.event_id,
                "entity_id": r.entity_id,
                "event_timestamp": r.event_timestamp,
                "schema_version": r.schema_version,
                **r.features,
            }
            for r in rows
        ]
    return pd.DataFrame.from_records(records, columns=FEATURE_COLUMNS)


def extract_labels(labels_watermark: datetime, upper_bound: datetime) -> pd.DataFrame:
    stmt = (
        select(HiggsLabelEvent)
        .where(HiggsLabelEvent.ingested_at > labels_watermark)
        .where(HiggsLabelEvent.ingested_at <= upper_bound)
        .order_by(HiggsLabelEvent.ingested_at)
    )
    with session_scope() as session:
        rows = session.execute(stmt).scalars().all()
        records = [
            {
                "event_id": r.event_id,
                "entity_id": r.entity_id,
                "target": r.target,
                "label_timestamp": r.label_timestamp,
                "schema_version": r.schema_version,
            }
            for r in rows
        ]
    return pd.DataFrame.from_records(records, columns=LABEL_COLUMNS)


def validate_features(df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationReport]:
    return validate(df, features_schema())


def validate_labels(df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationReport]:
    return validate(df, labels_schema())


def transform_features(df: pd.DataFrame) -> pd.DataFrame:
    """bronze keeps `features` nested (the shape it came from Postgres
    in); silver flattens it into one typed float column per HIGGS
    feature — a real "raw → analytics-ready" transform, not busywork.
    """
    out = df.copy()
    for col in HIGGS_FEATURE_NAMES:
        out[col] = out[col].astype("float64")
    out["event_timestamp"] = pd.to_datetime(out["event_timestamp"])
    return out.sort_values("event_id").reset_index(drop=True)


def transform_labels(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["target"] = out["target"].astype("int64")
    out["label_timestamp"] = pd.to_datetime(out["label_timestamp"])
    return out.sort_values("event_id").reset_index(drop=True)


def update_curated_features(session: Session, df: pd.DataFrame, run_id: str) -> int:
    """Idempotent upsert into `curated.higgs_features`. Returns the
    number of rows actually inserted (conflicts on an already-curated
    `event_id` are no-ops, mirroring the Milestone 1 sink pattern).
    """
    if df.empty:
        return 0
    records = df.to_dict(orient="records")
    rows = [
        {
            "event_id": r["event_id"],
            "entity_id": r["entity_id"],
            "event_timestamp": r["event_timestamp"],
            "schema_version": r["schema_version"],
            "features": {name: float(r[name]) for name in HIGGS_FEATURE_NAMES},
            "source_run_id": run_id,
        }
        for r in records
    ]
    total_inserted = 0
    for batch in _chunked(rows, UPSERT_BATCH_SIZE):
        stmt = (
            pg_insert(CuratedHiggsFeature)
            .values(batch)
            .on_conflict_do_nothing(index_elements=["event_id"])
            .returning(CuratedHiggsFeature.event_id)
        )
        total_inserted += len(session.execute(stmt).fetchall())
    return total_inserted


def update_curated_labels(session: Session, df: pd.DataFrame, run_id: str) -> int:
    if df.empty:
        return 0
    records = df.to_dict(orient="records")
    rows = [
        {
            "event_id": r["event_id"],
            "entity_id": r["entity_id"],
            "target": int(r["target"]),
            "label_timestamp": r["label_timestamp"],
            "schema_version": r["schema_version"],
            "source_run_id": run_id,
        }
        for r in records
    ]
    total_inserted = 0
    for batch in _chunked(rows, UPSERT_BATCH_SIZE):
        stmt = (
            pg_insert(CuratedHiggsLabel)
            .values(batch)
            .on_conflict_do_nothing(index_elements=["event_id"])
            .returning(CuratedHiggsLabel.event_id)
        )
        total_inserted += len(session.execute(stmt).fetchall())
    return total_inserted


def update_curated_tables_and_advance_watermarks(
    features_df: pd.DataFrame,
    labels_df: pd.DataFrame,
    run_id: str,
    upper_bound: datetime,
) -> tuple[int, int]:
    """The atomicity boundary: curated upserts and both watermark
    advances happen in one transaction. If anything here raises, nothing
    commits — the watermark stays put and a retry safely reprocesses the
    same window (the upserts are idempotent regardless).
    """
    with session_scope() as session:
        features_upserted = update_curated_features(session, features_df, run_id)
        labels_upserted = update_curated_labels(session, labels_df, run_id)
        advance_watermark(session, FEATURES_PIPELINE, upper_bound)
        advance_watermark(session, LABELS_PIPELINE, upper_bound)
    return features_upserted, labels_upserted


def build_training_dataset(run_id: str) -> pd.DataFrame:
    """Joins the *accumulated* `curated.higgs_features`/`higgs_labels`
    (not this run's extraction delta) for entities not yet in
    `curated.training_records` — so a feature whose label arrives in a
    later run still gets picked up. See LEARNING_LOG.md's Milestone 2
    entry for why this must join against curated state.
    """
    stmt = (
        select(CuratedHiggsFeature, CuratedHiggsLabel)
        .join(CuratedHiggsLabel, CuratedHiggsFeature.entity_id == CuratedHiggsLabel.entity_id)
        .outerjoin(
            CuratedTrainingRecord, CuratedHiggsFeature.entity_id == CuratedTrainingRecord.entity_id
        )
        .where(CuratedTrainingRecord.entity_id.is_(None))
    )
    with session_scope() as session:
        pairs = session.execute(stmt).all()
        records = [
            {
                "entity_id": feat.entity_id,
                "feature_event_id": feat.event_id,
                "label_event_id": label.event_id,
                "event_timestamp": feat.event_timestamp,
                "features": feat.features,
                "target": label.target,
                "schema_version": feat.schema_version,
                "source_run_id": run_id,
            }
            for feat, label in pairs
        ]
    return pd.DataFrame(records)


def upsert_training_records(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = df.to_dict(orient="records")
    total_inserted = 0
    with session_scope() as session:
        for batch in _chunked(rows, UPSERT_BATCH_SIZE):
            stmt = (
                pg_insert(CuratedTrainingRecord)
                .values(batch)
                .on_conflict_do_nothing(index_elements=["entity_id"])
                .returning(CuratedTrainingRecord.entity_id)
            )
            total_inserted += len(session.execute(stmt).fetchall())
    return total_inserted
