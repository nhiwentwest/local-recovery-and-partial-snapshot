#!/bin/bash
set -e

BOOTSTRAP="localhost:9092"
TOPICS=("p1.orders" "p1.orders.enriched" "p1.orders.output")

echo "📡 Creating topics on Kafka ($BOOTSTRAP)..."

for t in "${TOPICS[@]}"; do
  docker exec -it kafka kafka-topics \
    --bootstrap-server $BOOTSTRAP \
    --create --topic "$t" \
    --partitions 3 --replication-factor 1 || true
done

echo "✅ Topics created successfully."

