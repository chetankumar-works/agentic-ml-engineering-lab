"""Bronze → silver → gold batch pipeline for the HIGGS landing data.

    determine_high_watermark -> extract_new_records -> write_bronze
    -> validate -> transform -> write_silver -> update_curated_tables
    -> build_training_dataset -> update_feature_store (Feast -> Redis)
    -> emit_pipeline_metadata

Business logic lives in `higgs_pipeline_tasks.py` (Airflow-independent,
unit-tested in the normal uv workspace venv — see
`infra/airflow/tests/`); this file is the thin Airflow wrapper: `@task`
decorators, XCom, retries/timeouts, the failure callback.

Idempotency: every task's data write is either keyed by `run_id`
(bronze/silver/gold object keys — re-running a run overwrites the same
key) or an `INSERT ... ON CONFLICT DO NOTHING` upsert (curated tables).
The watermark only advances after the curated upserts commit, in the
same transaction — see `update_curated_tables_and_advance_watermarks` in
the tasks module and LEARNING_LOG.md's Milestone 2 entry.

Local staging (`/opt/airflow/staging/<run_id>/`) is inter-task hand-off
scratch space, not the durable copy — bronze/silver Parquet actually
written to MinIO is. This relies on all tasks running on the same
machine (LocalExecutor, single container); a distributed executor would
need a shared/object-store hand-off instead — a known, documented
simplification (see ARCHITECTURE.md).
"""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime, timedelta
from typing import Any

import higgs_pipeline_tasks as tasks
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from amel_common.logging import configure_logging, get_logger
from amel_db.models import PipelineRun
from amel_db.session import session_scope
from amel_lake.object_store import ensure_buckets, get_s3_client, put_json, put_parquet
from sqlalchemy.dialects.postgresql import insert as pg_insert

from amel_lake import keys

configure_logging("higgs_pipeline", os.environ.get("LOG_LEVEL", "info"))
logger = get_logger(component="higgs_pipeline")

STAGING_DIR = os.environ.get("PIPELINE_STAGING_DIR", "/opt/airflow/staging")
MAX_INVALID_FRACTION = float(os.environ.get("VALIDATION_MAX_INVALID_FRACTION", "0.05"))
SCHEDULE = os.environ.get("HIGGS_PIPELINE_SCHEDULE", "*/5 * * * *")
FEAST_SERVER_URL = os.environ.get("FEAST_SERVER_URL", "http://feast-server:6566")


def _naive(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


def _upper_bound(context: dict[str, Any]) -> datetime:
    """`data_interval_end` is only populated for schedule-driven runs; a
    manually triggered run (`airflow dags trigger`, no explicit logical
    date) has no data interval at all, so fall back to the run's actual
    trigger time (`dag_run.run_after`) — still a well-defined, per-run-id
    deterministic upper bound, just not tied to the cron schedule.
    """
    data_interval_end = context.get("data_interval_end")
    return _naive(data_interval_end) if data_interval_end else _naive(context["dag_run"].run_after)


def _run_dir(run_id: str) -> str:
    safe = run_id.replace(":", "_").replace("+", "_")
    path = os.path.join(STAGING_DIR, safe)
    os.makedirs(path, exist_ok=True)
    return path


def _record_pipeline_run(**fields: Any) -> None:
    with session_scope() as session:
        stmt = pg_insert(PipelineRun).values(**fields)
        update_cols = {k: v for k, v in fields.items() if k != "run_id"}
        stmt = stmt.on_conflict_do_update(index_elements=["run_id"], set_=update_cols)
        session.execute(stmt)


def _on_failure(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    run_id = context["run_id"]
    dag_run = context["dag_run"]
    error = str(context.get("exception") or "unknown error")
    logger.error("pipeline_task_failed", task_id=ti.task_id, run_id=run_id, error=error)
    _record_pipeline_run(
        run_id=run_id,
        dag_id=context["dag"].dag_id,
        status="failed",
        error_message=f"{ti.task_id}: {error}"[:2000],
        started_at=_naive(dag_run.start_date)
        if dag_run.start_date
        else datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": _on_failure,
}


@dag(
    dag_id="higgs_pipeline",
    description="Bronze/silver/gold batch processing of HIGGS landing data",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["milestone-2", "bronze-silver-gold"],
)
def higgs_pipeline() -> None:
    @task
    def determine_high_watermark_task(**context: Any) -> dict[str, str]:
        upper_bound = _upper_bound(context)
        bounds = tasks.determine_high_watermark(upper_bound)
        return {
            "features_watermark": bounds.features_watermark.isoformat(),
            "labels_watermark": bounds.labels_watermark.isoformat(),
            "upper_bound": bounds.upper_bound.isoformat(),
        }

    @task
    def extract_new_records_task(bounds: dict[str, str], **context: Any) -> dict[str, Any]:
        run_dir = _run_dir(context["run_id"])
        features_df = tasks.extract_features(
            datetime.fromisoformat(bounds["features_watermark"]),
            datetime.fromisoformat(bounds["upper_bound"]),
        )
        labels_df = tasks.extract_labels(
            datetime.fromisoformat(bounds["labels_watermark"]),
            datetime.fromisoformat(bounds["upper_bound"]),
        )
        features_path = os.path.join(run_dir, "raw_features.parquet")
        labels_path = os.path.join(run_dir, "raw_labels.parquet")
        features_df.to_parquet(features_path, index=False)
        labels_df.to_parquet(labels_path, index=False)
        logger.info("extracted_new_records", features=len(features_df), labels=len(labels_df))
        return {
            "features_path": features_path,
            "labels_path": labels_path,
            "features_count": len(features_df),
            "labels_count": len(labels_df),
        }

    @task
    def write_bronze_task(extracted: dict[str, Any], **context: Any) -> dict[str, str]:
        run_id = context["run_id"]
        event_date = _upper_bound(context).date()
        client = get_s3_client()
        ensure_buckets(client)

        f_key = keys.bronze_key("features", event_date, run_id)
        l_key = keys.bronze_key("labels", event_date, run_id)
        put_parquet(client, "bronze", f_key, pd.read_parquet(extracted["features_path"]))
        put_parquet(client, "bronze", l_key, pd.read_parquet(extracted["labels_path"]))
        return {"bronze_features_key": f_key, "bronze_labels_key": l_key}

    @task
    def validate_task(
        extracted: dict[str, Any], _bronze: dict[str, str], **context: Any
    ) -> dict[str, Any]:
        run_id = context["run_id"]
        run_dir = os.path.dirname(extracted["features_path"])

        valid_features, features_report = tasks.validate_features(
            pd.read_parquet(extracted["features_path"])
        )
        valid_labels, labels_report = tasks.validate_labels(
            pd.read_parquet(extracted["labels_path"])
        )

        valid_features_path = os.path.join(run_dir, "valid_features.parquet")
        valid_labels_path = os.path.join(run_dir, "valid_labels.parquet")
        valid_features.to_parquet(valid_features_path, index=False)
        valid_labels.to_parquet(valid_labels_path, index=False)

        report_key = keys.validation_report_key(run_id)
        put_json(
            get_s3_client(),
            "pipeline",
            report_key,
            {"features": features_report.to_dict(), "labels": labels_report.to_dict()},
        )

        if (
            features_report.invalid_fraction > MAX_INVALID_FRACTION
            or labels_report.invalid_fraction > MAX_INVALID_FRACTION
        ):
            raise AirflowException(
                f"validation gate failed: features invalid_fraction="
                f"{features_report.invalid_fraction:.4f}, labels invalid_fraction="
                f"{labels_report.invalid_fraction:.4f} (threshold={MAX_INVALID_FRACTION})"
            )

        return {
            "valid_features_path": valid_features_path,
            "valid_labels_path": valid_labels_path,
            "invalid_features_count": features_report.invalid_rows,
            "invalid_labels_count": labels_report.invalid_rows,
            "validation_report_key": report_key,
        }

    @task
    def transform_task(validated: dict[str, Any]) -> dict[str, str]:
        run_dir = os.path.dirname(validated["valid_features_path"])
        silver_features = tasks.transform_features(
            pd.read_parquet(validated["valid_features_path"])
        )
        silver_labels = tasks.transform_labels(pd.read_parquet(validated["valid_labels_path"]))

        silver_features_path = os.path.join(run_dir, "silver_features.parquet")
        silver_labels_path = os.path.join(run_dir, "silver_labels.parquet")
        silver_features.to_parquet(silver_features_path, index=False)
        silver_labels.to_parquet(silver_labels_path, index=False)
        return {
            "silver_features_path": silver_features_path,
            "silver_labels_path": silver_labels_path,
        }

    @task
    def write_silver_task(transformed: dict[str, str], **context: Any) -> dict[str, str]:
        run_id = context["run_id"]
        event_date = _upper_bound(context).date()
        client = get_s3_client()

        f_key = keys.silver_key("features", event_date, run_id)
        l_key = keys.silver_key("labels", event_date, run_id)
        put_parquet(client, "silver", f_key, pd.read_parquet(transformed["silver_features_path"]))
        put_parquet(client, "silver", l_key, pd.read_parquet(transformed["silver_labels_path"]))
        return {
            "silver_features_key": f_key,
            "silver_labels_key": l_key,
            "silver_features_path": transformed["silver_features_path"],
            "silver_labels_path": transformed["silver_labels_path"],
        }

    @task
    def update_curated_tables_task(
        silver: dict[str, str], bounds: dict[str, str], **context: Any
    ) -> dict[str, int]:
        upper_bound = datetime.fromisoformat(bounds["upper_bound"])
        features_upserted, labels_upserted = tasks.update_curated_tables_and_advance_watermarks(
            pd.read_parquet(silver["silver_features_path"]),
            pd.read_parquet(silver["silver_labels_path"]),
            context["run_id"],
            upper_bound,
        )
        return {
            "curated_features_upserted": features_upserted,
            "curated_labels_upserted": labels_upserted,
        }

    @task
    def build_training_dataset_task(_curated: dict[str, int], **context: Any) -> dict[str, Any]:
        run_id = context["run_id"]
        df = tasks.build_training_dataset(run_id)
        new_count = tasks.upsert_training_records(df)

        gold_key = None
        if new_count > 0:
            gold_key = keys.gold_training_key(run_id)
            put_parquet(get_s3_client(), "gold", gold_key, df)
        return {"training_records_new": new_count, "gold_training_key": gold_key}

    @task
    def update_feature_store_task(_gold: dict[str, Any], **context: Any) -> dict[str, Any]:
        # Runs after build_training_dataset only for ordering/clarity — it
        # reads this run's rows from curated.higgs_features (by
        # source_run_id), not the gold dataset. See tasks.update_feature_store.
        run_id = context["run_id"]
        result = tasks.update_feature_store(run_id, FEAST_SERVER_URL)
        logger.info("update_feature_store_done", run_id=run_id, **result)
        return result

    @task
    def emit_pipeline_metadata_task(
        bounds: dict[str, str],
        extracted: dict[str, Any],
        bronze: dict[str, str],
        validated: dict[str, Any],
        silver: dict[str, str],
        curated: dict[str, int],
        gold: dict[str, Any],
        _feature_store: dict[str, Any],
        **context: Any,
    ) -> None:
        run_id = context["run_id"]
        dag_run = context["dag_run"]
        upper_bound = datetime.fromisoformat(bounds["upper_bound"])

        _record_pipeline_run(
            run_id=run_id,
            dag_id=context["dag"].dag_id,
            status="success",
            features_watermark_before=datetime.fromisoformat(bounds["features_watermark"]),
            features_watermark_after=upper_bound,
            labels_watermark_before=datetime.fromisoformat(bounds["labels_watermark"]),
            labels_watermark_after=upper_bound,
            extracted_features=extracted["features_count"],
            extracted_labels=extracted["labels_count"],
            invalid_features=validated["invalid_features_count"],
            invalid_labels=validated["invalid_labels_count"],
            curated_features_upserted=curated["curated_features_upserted"],
            curated_labels_upserted=curated["curated_labels_upserted"],
            training_records_new=gold["training_records_new"],
            bronze_features_key=bronze["bronze_features_key"],
            bronze_labels_key=bronze["bronze_labels_key"],
            silver_features_key=silver["silver_features_key"],
            silver_labels_key=silver["silver_labels_key"],
            gold_training_key=gold["gold_training_key"],
            validation_report_key=validated["validation_report_key"],
            started_at=_naive(dag_run.start_date)
            if dag_run.start_date
            else datetime.now(UTC).replace(tzinfo=None),
            finished_at=datetime.now(UTC).replace(tzinfo=None),
        )
        logger.info("pipeline_run_recorded", run_id=run_id, status="success")

        shutil.rmtree(os.path.dirname(extracted["features_path"]), ignore_errors=True)

    bounds = determine_high_watermark_task()
    extracted = extract_new_records_task(bounds)
    bronze = write_bronze_task(extracted)
    validated = validate_task(extracted, bronze)
    transformed = transform_task(validated)
    silver = write_silver_task(transformed)
    curated = update_curated_tables_task(silver, bounds)
    gold = build_training_dataset_task(curated)
    feature_store = update_feature_store_task(gold)
    emit_pipeline_metadata_task(
        bounds, extracted, bronze, validated, silver, curated, gold, feature_store
    )


higgs_pipeline()
