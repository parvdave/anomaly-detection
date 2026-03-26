#!/bin/bash
set -e

KAFKA_BOOTSTRAP="kafka:9093"
MAX_RETRIES=30
RETRY_INTERVAL=2

echo "Waiting for Kafka to be ready..."
for i in $(seq 1 $MAX_RETRIES); do
  if kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list > /dev/null 2>&1; then
    echo "Kafka is ready."
    break
  fi
  echo "Attempt $i/$MAX_RETRIES: Kafka not ready yet, retrying in ${RETRY_INTERVAL}s..."
  sleep "$RETRY_INTERVAL"
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "ERROR: Kafka did not become ready in time."
    exit 1
  fi
done

echo "Creating topic: crypto.prices.raw"
kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic crypto.prices.raw \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete

echo "Creating topic: crypto.anomalies.detected"
kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic crypto.anomalies.detected \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=86400000

echo "Topics created:"
kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list
