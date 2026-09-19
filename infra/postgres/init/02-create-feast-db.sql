-- Feast's own SQL registry gets its own database on the same Postgres
-- instance, same pattern as Airflow's metadata DB (see DECISIONS.md
-- ADR-0004) — isolates Feast's internal registry tables from AMEL's own
-- schemas without a second Postgres container.
CREATE DATABASE feast;
