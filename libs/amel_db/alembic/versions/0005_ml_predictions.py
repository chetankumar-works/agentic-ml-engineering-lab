"""ml.predictions — one row per served prediction

Revision ID: 0005
Revises: 0004
Create Date: 2026-09-19

Milestone 5 (inference API): prediction metadata is persisted *before*
the prediction event is published to Kafka, so the table is the system
of record and the topic is a notification. Indexed by entity_id (per-
entity history), model_version (what did version N serve?) and
created_at (time-range queries for monitoring).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "predictions",
        sa.Column("prediction_id", sa.String(length=36), primary_key=True),
        sa.Column("entity_id", sa.String(length=64), nullable=True),
        sa.Column("source", sa.String(length=16), nullable=False),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("model_version", sa.String(length=32), nullable=False),
        sa.Column("model_run_id", sa.String(length=64), nullable=True),
        sa.Column("features", sa.JSON(), nullable=False),
        sa.Column("prediction", sa.SmallInteger(), nullable=False),
        sa.Column("probability", sa.Float(), nullable=False),
        sa.Column("latency_ms", sa.Float(), nullable=False),
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        schema="ml",
    )
    op.create_index("ix_ml_predictions_entity_id", "predictions", ["entity_id"], schema="ml")
    op.create_index(
        "ix_ml_predictions_model_version", "predictions", ["model_version"], schema="ml"
    )
    op.create_index("ix_ml_predictions_created_at", "predictions", ["created_at"], schema="ml")


def downgrade() -> None:
    op.drop_table("predictions", schema="ml")
