-- Airflow gets its own database on the same Postgres instance (not a
-- separate container): keeps infra minimal while still isolating
-- Airflow's ~50 internal metadata tables from AMEL's own control/
-- landing/curated/ml/audit/finops schemas in the `amel` database.
-- Runs once, on first container init (postgres-data volume empty) —
-- see infra/docker-compose.yml.
CREATE DATABASE airflow;
