"""create schema namespaces and landing tables

Revision ID: 0001
Revises:
Create Date: 2026-09-18

Creates the six schema namespaces the whole platform will use
(control, landing, curated, ml, audit, finops — see the Postgres section
of AMEL_KICKOFF_PROMPT.md) and the two `landing` tables Milestone 1
actually populates. Tables for the other schemas are added by later
migrations when the milestone that needs them is built — see
DECISIONS.md / PROJECT_STATE.md.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

SCHEMAS = ("control", "landing", "curated", "ml", "audit", "finops")


def upgrade() -> None:
    for schema in SCHEMAS:
        op.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    op.create_table(
        "higgs_feature_events",
        sa.Column("event_id", sa.String(length=64), primary_key=True),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(), nullable=False),
        sa.Column("schema_version", sa.String(length=16), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("features", sa.JSON(), nullable=False),
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.Integer(), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema="landing",
    )
    op.create_index(
        "ix_landing_higgs_feature_events_entity_id",
        "higgs_feature_events",
        ["entity_id"],
        schema="landing",
    )
    op.create_index(
        "ix_landing_higgs_feature_events_event_timestamp",
        "higgs_feature_events",
        ["event_timestamp"],
        schema="landing",
    )

    op.create_table(
        "higgs_label_events",
        sa.Column("event_id", sa.String(length=64), primary_key=True),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("target", sa.SmallInteger(), nullable=False),
        sa.Column("label_timestamp", sa.DateTime(), nullable=False),
        sa.Column("schema_version", sa.String(length=16), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.Integer(), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema="landing",
    )
    op.create_index(
        "ix_landing_higgs_label_events_entity_id",
        "higgs_label_events",
        ["entity_id"],
        schema="landing",
    )
    op.create_index(
        "ix_landing_higgs_label_events_label_timestamp",
        "higgs_label_events",
        ["label_timestamp"],
        schema="landing",
    )


def downgrade() -> None:
    op.drop_table("higgs_label_events", schema="landing")
    op.drop_table("higgs_feature_events", schema="landing")
    for schema in SCHEMAS:
        op.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
