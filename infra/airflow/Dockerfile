# Airflow gets its own image, deliberately OUTSIDE the uv workspace (see
# DECISIONS.md ADR-0002's anticipated trigger: Airflow's own dependency
# pins are strict enough that sharing one lockfile with the rest of the
# monorepo isn't worth it). amel_common/amel_db/amel_lake are installed
# as plain pip packages from their own self-contained pyproject.toml —
# each also remains a uv workspace member so `make test`/`make lint` at
# the repo root still cover them without Airflow itself in the loop.
#
# Build context is the repo root (see infra/docker-compose.yml).
FROM apache/airflow:3.3.2-python3.12

USER airflow

COPY --chown=airflow:root libs/amel_common /opt/build/libs/amel_common
COPY --chown=airflow:root libs/amel_db /opt/build/libs/amel_db
COPY --chown=airflow:root libs/amel_lake /opt/build/libs/amel_lake

# --no-deps for our own path-based packages: pip has no index entry for
# "amel-common"/"amel-db" (they're private, path-installed), so
# resolving their declared dependencies is done explicitly below instead
# of letting pip try (and fail) to look them up on PyPI.
RUN pip install --no-cache-dir --no-deps \
        /opt/build/libs/amel_common \
        /opt/build/libs/amel_db \
        /opt/build/libs/amel_lake \
    && pip install --no-cache-dir \
        "pydantic>=2.9" "pydantic-settings>=2.6" "structlog>=24.4" \
        "sqlalchemy>=2.0" "alembic>=1.13" "psycopg[binary]>=3.2" \
        "boto3>=1.35" "pandas>=2.2" "pyarrow>=17.0" "pandera[pandas]>=0.20"

COPY --chown=airflow:root infra/airflow/dags /opt/airflow/dags

# Pre-create with airflow-user ownership so it's inherited when Docker
# populates the named `airflow-staging` volume from the image on first
# mount — otherwise the mountpoint defaults to root-owned and every task
# writing inter-task hand-off Parquet here gets PermissionError.
RUN mkdir -p /opt/airflow/staging
