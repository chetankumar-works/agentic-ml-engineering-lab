#!/usr/bin/env bash
# Delete the kind cluster and hand the four services back to Compose.
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f infra/docker-compose.yml"
kind delete cluster --name amel 2>/dev/null || true
MAXID=$(docker exec amel-postgres-1 psql -U amel -d amel -tAc \
  "SELECT coalesce(max(entity_id),'higgs-000000000') FROM landing.higgs_feature_events" 2>/dev/null || echo higgs-000000000)
HIGGS_START_INDEX=$(( 10#${MAXID#higgs-} + 1 )) $COMPOSE up -d --no-deps airflow feast-server inference-api source-simulator stream-ingestor
