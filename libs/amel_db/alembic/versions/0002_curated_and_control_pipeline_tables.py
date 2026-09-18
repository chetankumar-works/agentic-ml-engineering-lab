"""curated tables, control pipeline-tracking tables, landing ingested_at indexes

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-25

Milestone 2 (Airflow bronze/silver/gold batch pipeline): adds
`curated.higgs_features`, `curated.higgs_labels`,
`curated.training_records`, `control.pipeline_watermarks`,
`control.pipeline_runs`; and indexes `landing.*.ingested_at`, which the
pipeline's watermark-based extraction queries on
(`WHERE ingested_at > watermark AND ingested_at <= upper_bound`) — those
tables existed since migration 0001 but without this index, since
Milestone 1 never queried by `ingested_at`.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_landing_higgs_feature_events_ingested_at",
        "higgs_feature_events",
        ["ingested_at"],
        schema="landing",
    )
    op.create_index(
        "ix_landing_higgs_label_events_ingested_at",
        "higgs_label_events",
        ["ingested_at"],
        schema="landing",
    )

    op.create_table(
        "higgs_features",
        sa.Column("event_id", sa.String(length=64), primary_key=True),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(), nullable=False),
        sa.Column("schema_version", sa.String(length=16), nullable=False),
        sa.Column("features", sa.JSON(), nullable=False),
        sa.Column("source_run_id", sa.String(length=128), nullable=False),
        sa.Column("loaded_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        schema="curated",
    )
    op.create_index(
        "ix_curated_higgs_features_entity_id", "higgs_features", ["entity_id"], schema="curated"
    )
    op.create_index(
        "ix_curated_higgs_features_event_timestamp",
        "higgs_features",
        ["event_timestamp"],
        schema="curated",
    )

    op.create_table(
        "higgs_labels",
        sa.Column("event_id", sa.String(length=64), primary_key=True),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("target", sa.SmallInteger(), nullable=False),
        sa.Column("label_timestamp", sa.DateTime(), nullable=False),
        sa.Column("schema_version", sa.String(length=16), nullable=False),
        sa.Column("source_run_id", sa.String(length=128), nullable=False),
        sa.Column("loaded_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        schema="curated",
    )
    op.create_index(
        "ix_curated_higgs_labels_entity_id", "higgs_labels", ["entity_id"], schema="curated"
    )
    op.create_index(
        "ix_curated_higgs_labels_label_timestamp",
        "higgs_labels",
        ["label_timestamp"],
        schema="curated",
    )

    op.create_table(
        "training_records",
        sa.Column("entity_id", sa.String(length=64), primary_key=True),
        sa.Column("feature_event_id", sa.String(length=64), nullable=False),
        sa.Column("label_event_id", sa.String(length=64), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(), nullable=False),
        sa.Column("features", sa.JSON(), nullable=False),
        sa.Column("target", sa.SmallInteger(), nullable=False),
        sa.Column("schema_version", sa.String(length=16), nullable=False),
        sa.Column("source_run_id", sa.String(length=128), nullable=False),
        sa.Column("loaded_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        schema="curated",
    )
    op.create_index(
        "ix_curated_training_records_event_timestamp",
        "training_records",
        ["event_timestamp"],
        schema="curated",
    )

    op.create_table(
        "pipeline_watermarks",
        sa.Column("pipeline_name", sa.String(length=128), primary_key=True),
        sa.Column("watermark", sa.DateTime(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            onupdate=sa.text("now()"),
            nullable=False,
        ),
        schema="control",
    )

    op.create_table(
        "pipeline_runs",
        sa.Column("run_id", sa.String(length=128), primary_key=True),
        sa.Column("dag_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("features_watermark_before", sa.DateTime(), nullable=True),
        sa.Column("features_watermark_after", sa.DateTime(), nullable=True),
        sa.Column("labels_watermark_before", sa.DateTime(), nullable=True),
        sa.Column("labels_watermark_after", sa.DateTime(), nullable=True),
        sa.Column("extracted_features", sa.Integer(), nullable=True),
        sa.Column("extracted_labels", sa.Integer(), nullable=True),
        sa.Column("invalid_features", sa.Integer(), nullable=True),
        sa.Column("invalid_labels", sa.Integer(), nullable=True),
        sa.Column("curated_features_upserted", sa.Integer(), nullable=True),
        sa.Column("curated_labels_upserted", sa.Integer(), nullable=True),
        sa.Column("training_records_new", sa.Integer(), nullable=True),
        sa.Column("bronze_features_key", sa.String(length=512), nullable=True),
        sa.Column("bronze_labels_key", sa.String(length=512), nullable=True),
        sa.Column("silver_features_key", sa.String(length=512), nullable=True),
        sa.Column("silver_labels_key", sa.String(length=512), nullable=True),
        sa.Column("gold_training_key", sa.String(length=512), nullable=True),
        sa.Column("validation_report_key", sa.String(length=512), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        schema="control",
    )


def downgrade() -> None:
    op.drop_table("pipeline_runs", schema="control")
    op.drop_table("pipeline_watermarks", schema="control")
    op.drop_table("training_records", schema="curated")
    op.drop_table("higgs_labels", schema="curated")
    op.drop_table("higgs_features", schema="curated")
    op.drop_index(
        "ix_landing_higgs_label_events_ingested_at",
        table_name="higgs_label_events",
        schema="landing",
    )
    op.drop_index(
        "ix_landing_higgs_feature_events_ingested_at",
        table_name="higgs_feature_events",
        schema="landing",
    )
