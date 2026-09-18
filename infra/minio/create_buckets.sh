#!/bin/sh
# One-shot bucket bootstrap, run as the `minio-init` compose service.
# `mc mb --ignore-existing` makes re-running (e.g. `docker compose up`
# again) never fail just because the buckets already exist — matches
# infra/kafka/create_topics.sh's idempotency pattern.
set -eu

ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
ACCESS_KEY="${MINIO_ROOT_USER:-amel}"
SECRET_KEY="${MINIO_ROOT_PASSWORD:-amel_dev_password}"

mc alias set local "$ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY"

for bucket in bronze silver gold mlflow pipeline; do
  echo "creating bucket ${bucket}"
  mc mb --ignore-existing "local/${bucket}"
done

echo "all buckets ready:"
mc ls local
