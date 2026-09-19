"""curated.higgs_features_flat view (Feast offline source)

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-19

Feast's Postgres offline store reads feature values from a table/view
with one column per feature — `curated.higgs_features.features` is a
JSON blob (matching the nested shape events arrive in), so this
migration adds a VIEW that flattens it into 28 typed float columns
without duplicating storage. Generated from
`amel_common.schemas.HIGGS_FEATURE_NAMES` so the view can never drift
out of sync with the feature list itself.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from amel_common.schemas import HIGGS_FEATURE_NAMES

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None

_FEATURE_COLUMNS_SQL = ",\n    ".join(
    f"(features->>'{name}')::float8 AS {name}" for name in HIGGS_FEATURE_NAMES
)


def upgrade() -> None:
    op.execute(
        f"""
        CREATE VIEW curated.higgs_features_flat AS
        SELECT
            event_id,
            entity_id,
            event_timestamp,
            schema_version,
            {_FEATURE_COLUMNS_SQL}
        FROM curated.higgs_features
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW curated.higgs_features_flat")
