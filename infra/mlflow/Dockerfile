# MLflow tracking server + model registry. Its own small image (not the
# uv workspace): the *server* only needs mlflow + a Postgres driver + an
# S3 client, and pinning it to the same mlflow version the training
# package resolved in uv.lock keeps client/server in step.
#
# Backend store: the `mlflow` database on the shared Postgres (see
# infra/postgres/init/03-create-mlflow-db.sql, DECISIONS.md ADR-0006).
# Artifacts: MinIO bucket `mlflow` (created by minio-init), proxied by
# the server (`--serve-artifacts`) so clients need no MinIO credentials.
FROM python:3.12-slim-bookworm

ARG MLFLOW_VERSION=3.16.1
RUN pip install --no-cache-dir "mlflow==${MLFLOW_VERSION}" "psycopg2-binary>=2.9" "boto3>=1.35"

EXPOSE 5000
ENTRYPOINT ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
