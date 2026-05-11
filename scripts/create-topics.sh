#!/usr/bin/env bash
# Create Redpanda topics for Muninn.
# Usage: ./scripts/create-topics.sh
# Requires: rpk (Redpanda CLI) or docker exec with Redpanda container

set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}"

TOPICS=(
  "events.trade"
  "events.book.snapshot"
  "events.candle"
  "events.deadletter"
  "features.vwap.v1"
)

echo "Creating Muninn topics on broker: $BROKER"

for topic in "${TOPICS[@]}"; do
  echo "  Creating topic: $topic"
  if command -v rpk &> /dev/null; then
    rpk topic create "$topic" --brokers "$BROKER" --partitions 1 --replicas 1 2>/dev/null || true
  else
    docker exec muninn-redpanda-1 rpk topic create "$topic" --brokers "localhost:9092" --partitions 1 --replicas 1 2>/dev/null || true
  fi
done

echo "Topics created successfully."
echo ""
echo "Listing topics:"
if command -v rpk &> /dev/null; then
  rpk topic list --brokers "$BROKER"
else
  docker exec muninn-redpanda-1 rpk topic list --brokers "localhost:9092"
fi
