"""ml.model_promotions — the audit trail for candidate -> champion promotions

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-19

Milestone 4 (training + MLflow): every promotion of a registered model
version to the `champion` alias is recorded here *in addition to* the
alias change in MLflow's registry. MLflow records the current state
(which version holds the alias); this table records the history — who
promoted what, when, from which previous champion, against which
criteria and metrics — which is what "explicit and auditable" means.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "model_promotions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=True),
        sa.Column("from_alias", sa.String(length=32), nullable=True),
        sa.Column("to_alias", sa.String(length=32), nullable=False),
        sa.Column("previous_champion_version", sa.String(length=32), nullable=True),
        sa.Column("decided_by", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("criteria", sa.JSON(), nullable=False),
        sa.Column("candidate_metrics", sa.JSON(), nullable=False),
        sa.Column("champion_metrics", sa.JSON(), nullable=True),
        sa.Column("promoted_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        schema="ml",
    )
    op.create_index(
        "ix_ml_model_promotions_model_name_promoted_at",
        "model_promotions",
        ["model_name", "promoted_at"],
        schema="ml",
    )


def downgrade() -> None:
    op.drop_table("model_promotions", schema="ml")
