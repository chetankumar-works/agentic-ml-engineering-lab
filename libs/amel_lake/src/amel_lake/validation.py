"""Data validation layer for the bronze→silver batch pipeline (Milestone
2). Events landing in Postgres already passed Pydantic schema validation
at the Kafka ingestion boundary (Milestone 1) — nothing malformed reaches
here. This layer checks a different, business-level class of problem:
numeric sanity, timestamp sanity, and duplicate IDs *within an extracted
batch* — the kind of issue a schema-valid-but-still-wrong record can have
(e.g. a sensor producing wildly out-of-range values, a clock skew, an
upstream retry that duplicated a row before Milestone 1's own dedup
logic even saw it). Invalid rows are quarantined (excluded from the
valid subset) and reported, never silently dropped without a trace — see
the returned `ValidationReport`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pandera.pandas as pa
from amel_common.schemas import HIGGS_FEATURE_NAMES

# Generous bounds: HIGGS features are physics quantities roughly
# normalized to an O(1)-O(10) scale in the source dataset. These bounds
# exist to catch corruption (NaN/inf, a units bug, a decoding error), not
# to enforce a tight statistical prior on legitimate values.
_FEATURE_BOUNDS = (-50.0, 50.0)
# landing.*'s timestamp columns are `TIMESTAMP WITHOUT TIME ZONE`
# (Postgres strips tzinfo on read), so every timestamp handled by this
# batch pipeline is naive-but-implicitly-UTC — never tz-aware — all the
# way through bronze/silver/gold. Comparison bounds here are naive too,
# to match.
_MIN_EVENT_TIMESTAMP = datetime(2000, 1, 1)
_FUTURE_GRACE = timedelta(days=1)


def _max_event_timestamp() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None) + _FUTURE_GRACE


def features_schema() -> pa.DataFrameSchema:
    columns: dict[str, pa.Column] = {
        "event_id": pa.Column(str, unique=True, nullable=False),
        "entity_id": pa.Column(str, nullable=False),
        "event_timestamp": pa.Column(
            "datetime64[ns]",
            checks=[
                pa.Check.ge(_MIN_EVENT_TIMESTAMP),
                pa.Check.le(_max_event_timestamp()),
            ],
            nullable=False,
        ),
    }
    for name in HIGGS_FEATURE_NAMES:
        columns[name] = pa.Column(
            float,
            checks=pa.Check.in_range(*_FEATURE_BOUNDS),
            nullable=False,
        )
    return pa.DataFrameSchema(columns, strict=False, coerce=False)


def labels_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "event_id": pa.Column(str, unique=True, nullable=False),
            "entity_id": pa.Column(str, nullable=False),
            "target": pa.Column(int, checks=pa.Check.isin([0, 1]), nullable=False),
            "label_timestamp": pa.Column(
                "datetime64[ns]",
                checks=[
                    pa.Check.ge(_MIN_EVENT_TIMESTAMP),
                    pa.Check.le(_max_event_timestamp()),
                ],
                nullable=False,
            ),
        },
        strict=False,
        coerce=False,
    )


@dataclass
class ValidationReport:
    total_rows: int
    valid_rows: int
    invalid_rows: int
    failures: list[dict[str, Any]] = field(default_factory=list)

    @property
    def invalid_fraction(self) -> float:
        return 0.0 if self.total_rows == 0 else self.invalid_rows / self.total_rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "invalid_rows": self.invalid_rows,
            "invalid_fraction": round(self.invalid_fraction, 6),
            "failures": self.failures,
        }


def validate(df: pd.DataFrame, schema: pa.DataFrameSchema) -> tuple[pd.DataFrame, ValidationReport]:
    """Returns (valid_subset, report). Never raises for row-level
    validation failures — the caller decides whether `invalid_fraction`
    is acceptable (see amel_lake's DAG task, which fails the pipeline run
    above a configurable threshold rather than silently proceeding).
    """
    total = len(df)
    if total == 0:
        return df, ValidationReport(total_rows=0, valid_rows=0, invalid_rows=0)

    try:
        schema.validate(df, lazy=True)
        return df, ValidationReport(total_rows=total, valid_rows=total, invalid_rows=0)
    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases
        invalid_index = set(failure_cases["index"].dropna().tolist())
        valid_df = df.drop(index=list(invalid_index))

        failures = (
            failure_cases.groupby(["column", "check"], dropna=False)
            .size()
            .reset_index(name="failure_count")
            .to_dict(orient="records")
        )
        return valid_df, ValidationReport(
            total_rows=total,
            valid_rows=len(valid_df),
            invalid_rows=len(invalid_index),
            failures=failures,
        )
