"""SQLAlchemy models for AMEL's PostgreSQL schemas.

The `landing` tables (Milestone 1: the stream ingestor's idempotent
sink), `curated` tables and `control` pipeline-tracking tables
(Milestone 2: the Airflow batch pipeline), the `ml` promotion audit
table (Milestone 4) and `ml.predictions` (Milestone 5) are defined
here. The `audit` and `finops` schema *namespaces* are provisioned by the initial migration (see
`alembic/versions/0001_...py`) but have no tables yet — added only when
the milestone that needs them is actually built — see ARCHITECTURE.md
and PROJECT_STATE.md for the build order. Do not add tables here
speculatively.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Float, Integer, SmallInteger, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class HiggsFeatureEvent(Base):
    """Landing-zone record of one HIGGS feature event consumed from
    `higgs.features.v1`. `event_id` is the primary key: the idempotent
    sink relies on this (via `INSERT ... ON CONFLICT (event_id) DO
    NOTHING`) to make redelivery safe — see LEARNING_LOG.md's Milestone 1
    entry for the full at-least-once + idempotent-sink argument.
    """

    __tablename__ = "higgs_feature_events"
    __table_args__ = {"schema": "landing"}

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    event_timestamp: Mapped[datetime] = mapped_column(index=True, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    features: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)
    kafka_partition: Mapped[int] = mapped_column(nullable=False)
    kafka_offset: Mapped[int] = mapped_column(nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False, index=True
    )


class HiggsLabelEvent(Base):
    """Landing-zone record of one HIGGS label event consumed from
    `higgs.labels.v1`, published separately from (and often after) its
    corresponding feature event — see LABEL_DELAY_SECONDS in the source
    simulator.
    """

    __tablename__ = "higgs_label_events"
    __table_args__ = {"schema": "landing"}

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    target: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    label_timestamp: Mapped[datetime] = mapped_column(index=True, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    kafka_partition: Mapped[int] = mapped_column(nullable=False)
    kafka_offset: Mapped[int] = mapped_column(nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False, index=True
    )


class CuratedHiggsFeature(Base):
    """Deduplicated, validated copy of a feature event, upserted by the
    Milestone 2 Airflow pipeline's `update_curated_tables` task from the
    `silver` Parquet zone. `source_run_id` gives lineage back to the DAG
    run that curated this row.
    """

    __tablename__ = "higgs_features"
    __table_args__ = {"schema": "curated"}

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    event_timestamp: Mapped[datetime] = mapped_column(index=True, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    features: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    source_run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)


class CuratedHiggsLabel(Base):
    """Deduplicated, validated copy of a label event — see
    `CuratedHiggsFeature`.
    """

    __tablename__ = "higgs_labels"
    __table_args__ = {"schema": "curated"}

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    entity_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    target: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    label_timestamp: Mapped[datetime] = mapped_column(index=True, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    source_run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)


class CuratedTrainingRecord(Base):
    """One row per entity that has *both* a curated feature and a
    curated label — built by `build_training_dataset` joining
    `curated.higgs_features`/`curated.higgs_labels` (the accumulated
    curated state, not just the current DAG run's delta), so a feature
    whose label arrives in a later run still gets picked up. See
    LEARNING_LOG.md's Milestone 2 entry for why this join is against
    curated state rather than the run's own extraction window.
    """

    __tablename__ = "training_records"
    __table_args__ = {"schema": "curated"}

    entity_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    feature_event_id: Mapped[str] = mapped_column(String(64), nullable=False)
    label_event_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_timestamp: Mapped[datetime] = mapped_column(index=True, nullable=False)
    features: Mapped[dict[str, float]] = mapped_column(JSON, nullable=False)
    target: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    source_run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)


class PipelineWatermark(Base):
    """The last-processed `ingested_at` boundary for one logical stream
    (`higgs_features` / `higgs_labels`), advanced only after the data up
    to that boundary is durably curated — see `update_curated_tables` in
    `infra/airflow/dags/higgs_pipeline_tasks.py`.
    """

    __tablename__ = "pipeline_watermarks"
    __table_args__ = {"schema": "control"}

    pipeline_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    watermark: Mapped[datetime] = mapped_column(nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now(), nullable=False
    )


class PipelineRun(Base):
    """One row per Airflow DAG run of the batch pipeline — written by the
    final `emit_pipeline_metadata` task (success) or the DAG's
    `on_failure_callback` (failure). This is the authoritative "what
    happened in this run" record; XCom values feeding it are intentionally
    small (counts, keys, timestamps), never the row data itself.
    """

    __tablename__ = "pipeline_runs"
    __table_args__ = {"schema": "control"}

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    dag_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    features_watermark_before: Mapped[datetime | None] = mapped_column(nullable=True)
    features_watermark_after: Mapped[datetime | None] = mapped_column(nullable=True)
    labels_watermark_before: Mapped[datetime | None] = mapped_column(nullable=True)
    labels_watermark_after: Mapped[datetime | None] = mapped_column(nullable=True)
    extracted_features: Mapped[int | None] = mapped_column(nullable=True)
    extracted_labels: Mapped[int | None] = mapped_column(nullable=True)
    invalid_features: Mapped[int | None] = mapped_column(nullable=True)
    invalid_labels: Mapped[int | None] = mapped_column(nullable=True)
    curated_features_upserted: Mapped[int | None] = mapped_column(nullable=True)
    curated_labels_upserted: Mapped[int | None] = mapped_column(nullable=True)
    training_records_new: Mapped[int | None] = mapped_column(nullable=True)
    bronze_features_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    bronze_labels_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    silver_features_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    silver_labels_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    gold_training_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    validation_report_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)


class ModelPromotion(Base):
    """Audit record of one alias promotion in the MLflow model registry
    (Milestone 4). MLflow holds the *current* alias→version mapping; this
    table holds the *history* with the decision context — criteria,
    both sides' metrics, who decided, why — so a champion change is
    explicit and reviewable after the fact, never a silent alias flip.
    """

    __tablename__ = "model_promotions"
    __table_args__ = {"schema": "ml"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_name: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    from_alias: Mapped[str | None] = mapped_column(String(32), nullable=True)
    to_alias: Mapped[str] = mapped_column(String(32), nullable=False)
    previous_champion_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    decided_by: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    criteria: Mapped[dict] = mapped_column(JSON, nullable=False)
    candidate_metrics: Mapped[dict] = mapped_column(JSON, nullable=False)
    champion_metrics: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    promoted_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)


class Prediction(Base):
    """One served prediction (Milestone 5). Written by `apps/inference_api`
    before the matching `PredictionEvent` is published to `predictions.v1`;
    the row is the system of record, the event is the notification.
    """

    __tablename__ = "predictions"
    __table_args__ = {"schema": "ml"}

    prediction_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    entity_id: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    source: Mapped[str] = mapped_column(String(16), nullable=False)
    model_name: Mapped[str] = mapped_column(String(128), nullable=False)
    model_version: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    model_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    features: Mapped[dict] = mapped_column(JSON, nullable=False)
    prediction: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    probability: Mapped[float] = mapped_column(Float, nullable=False)
    latency_ms: Mapped[float] = mapped_column(Float, nullable=False)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        index=True, server_default=func.now(), nullable=False
    )
