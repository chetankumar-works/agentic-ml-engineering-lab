-- MLflow tracking server backend store: its own database on the shared
-- Postgres instance (same pattern as airflow/feast — DECISIONS.md ADR-0004).
CREATE DATABASE mlflow;
