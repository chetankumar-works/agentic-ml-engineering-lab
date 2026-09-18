"""Watermark read/advance for the batch pipeline's watermark-based CDC
extraction (`landing.* WHERE ingested_at > watermark AND <= upper_bound`).

`advance_watermark` takes a caller-managed `Session` deliberately: the
watermark must advance in the *same* transaction as the curated upserts
it gates (see `update_curated_tables` in
`infra/airflow/dags/higgs_pipeline_tasks.py`) so a failure partway
through never leaves the watermark ahead of what was actually durably
curated — the same "commit data + advance checkpoint together" pattern
used for the Kafka offset commit in Milestone 1.

`advance_watermark` only ever moves the watermark *forward*
(`GREATEST(current, new)`), never overwrites it unconditionally. This
was discovered to matter in practice, not just in theory: several
retried DAG runs briefly executed concurrently during Milestone 2's
acceptance run (see LEARNING_LOG.md), and whichever one's transaction
happened to *commit* last — not necessarily the one with the latest
`upper_bound` — would otherwise have overwritten a more-advanced
watermark with an older value, silently regressing it. Widening a future
extraction window this way is wasteful (harmless re-extraction of
already-curated rows, deduped by the idempotent upsert) but is not
correct "high watermark" behavior, so it's guarded here directly rather
than relied upon to be harmless.

Watermarks are naive-but-implicitly-UTC datetimes, matching
`landing.*.ingested_at`'s `TIMESTAMP WITHOUT TIME ZONE` column type —
Postgres rejects comparing that type against a tz-aware value directly,
so this module never produces or accepts tz-aware datetimes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from amel_db.models import PipelineWatermark
from amel_db.session import session_scope
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

EPOCH = datetime(1970, 1, 1)


def utcnow_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def get_watermark(pipeline_name: str) -> datetime:
    with session_scope() as session:
        row = session.get(PipelineWatermark, pipeline_name)
        return row.watermark if row is not None else EPOCH


def _advance_watermark_stmt(pipeline_name: str, new_value: datetime) -> Any:
    stmt = pg_insert(PipelineWatermark).values(pipeline_name=pipeline_name, watermark=new_value)
    return stmt.on_conflict_do_update(
        index_elements=["pipeline_name"],
        set_={
            "watermark": func.greatest(PipelineWatermark.watermark, stmt.excluded.watermark),
            # The model's onupdate=func.now() only fires through the ORM
            # unit-of-work, not this raw Core `ON CONFLICT DO UPDATE` —
            # set it explicitly or `updated_at` silently goes stale.
            "updated_at": func.now(),
        },
    )


def advance_watermark(session: Session, pipeline_name: str, new_value: datetime) -> None:
    session.execute(_advance_watermark_stmt(pipeline_name, new_value))
