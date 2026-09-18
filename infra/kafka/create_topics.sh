#!/bin/bash
# One-shot topic bootstrap, run as the `kafka-init` compose service.
# Idempotent: --if-not-exists means re-running (e.g. `docker compose up`
# again) never fails just because the topics already exist.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPICS_BIN=/opt/kafka/bin/kafka-topics.sh

TOPICS=(
  "higgs.features.v1:6:1"
  "higgs.labels.v1:6:1"
  "higgs.features.dlq:3:1"
  "higgs.labels.dlq:3:1"
  "predictions.v1:6:1"
  "platform.events.v1:3:1"
  "audit.events.v1:3:1"
)

for entry in "${TOPICS[@]}"; do
  IFS=':' read -r name partitions replication <<< "$entry"
  echo "creating topic ${name} (partitions=${partitions}, replication=${replication})"
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$replication"
done

echo "all topics ready:"
"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --list
