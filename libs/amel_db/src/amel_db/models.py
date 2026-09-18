"""SQLAlchemy models for AMEL's PostgreSQL schemas.

Only the `landing` tables actually used by Milestone 1 (the stream
ingestor's idempotent sink) are defined here. The `control`, `curated`,
`ml`, `audit`, and `finops` schema *namespaces* are provisioned by the
initial migration (see `alembic/versions/0001_...py`) so later milestones
don't need a "create schema" migration of their own, but their tables are
added only when the milestone that needs them is actually built — see
ARCHITECTURE.md and PROJECT_STATE.md for the build order. Do not add
tables here speculatively.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, SmallInteger, String
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
    ingested_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)


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
    ingested_at: Mapped[datetime] = mapped_column(server_default=func.now(), nullable=False)
