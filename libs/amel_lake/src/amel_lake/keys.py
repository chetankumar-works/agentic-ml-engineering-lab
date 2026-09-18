"""Deterministic object-storage key builders. Keying bronze/silver/gold
objects by `run_id` (Airflow's own unique run identifier) makes writes
idempotent: retrying or re-running the same DAG run overwrites the same
key rather than accumulating duplicates — the object-storage equivalent
of the `ON CONFLICT DO NOTHING` idempotency pattern used for the Postgres
sink in Milestone 1.
"""

from __future__ import annotations

from datetime import date


def _safe_run_id(run_id: str) -> str:
    return run_id.replace(":", "_").replace("+", "_")


def bronze_key(dataset: str, event_date: date, run_id: str) -> str:
    return f"higgs/{dataset}/dt={event_date.isoformat()}/run_id={_safe_run_id(run_id)}.parquet"


def silver_key(dataset: str, event_date: date, run_id: str) -> str:
    return f"higgs/{dataset}/dt={event_date.isoformat()}/run_id={_safe_run_id(run_id)}.parquet"


def gold_training_key(run_id: str) -> str:
    return f"training/run_id={_safe_run_id(run_id)}.parquet"


def validation_report_key(run_id: str) -> str:
    return f"artifacts/validation_reports/run_id={_safe_run_id(run_id)}.json"
