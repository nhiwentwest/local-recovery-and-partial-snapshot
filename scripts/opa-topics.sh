#!/usr/bin/env bash
set -euo pipefail

# Create topics for p1 prefix using Redpanda rpk (or kafka-topics)
# Assumes docker-compose up -d started a container named 'redpanda'

if docker exec redpanda rpk cluster info >/dev/null 2>&1; then
  docker exec redpanda rpk topic create p1.orders --brokers redpanda:9092 || true
  docker exec redpanda rpk topic create p1.orders.enriched --brokers redpanda:9092 || true
  docker exec redpanda rpk topic create p1.orders.output --brokers redpanda:9092 || true
  echo "[rpk] ensured: p1.orders, p1.orders.enriched, p1.orders.output"
else
  echo "Redpanda container not ready. If you have kafka-topics, run:"
  echo "  kafka-topics --bootstrap-server localhost:19092 --create --topic p1.orders --partitions 3 --replication-factor 1"
  echo "  kafka-topics --bootstrap-server localhost:19092 --create --topic p1.orders.enriched --partitions 3 --replication-factor 1"
  echo "  kafka-topics --bootstrap-server localhost:19092 --create --topic p1.orders.output --partitions 3 --replication-factor 1"
  exit 1
fi

