#!/usr/bin/env bash
set -euo pipefail

# Usage: INSTANCE_ID=${INSTANCE_ID:-B2} HTTP=${HTTP:-http://127.0.0.1:8080/metrics} ./scripts/scale_out.sh

INSTANCE_ID=${INSTANCE_ID:-B2}
HTTP=${HTTP:-http://127.0.0.1:8080/metrics}

echo "[scale_out] Starting second OpB instance (INSTANCE_ID=${INSTANCE_ID}) in another terminal/session..."
echo "Please run: ./bin/opb -instance-id ${INSTANCE_ID} -kafka-bootstrap 127.0.0.1:9092 -input-source kafka -output-tx-id opb-${INSTANCE_ID}"

echo "[scale_out] Sampling partition lag for 30s..."
for i in {1..6}; do
  curl -s "$HTTP" | awk -F'[ {}]+' '/^opb_partition_lag/ {print $1" "$NF}' | sed 's/opb_partition_lag//'
  sleep 5
done

echo "[scale_out] Expectation: overall lag decreases after OpB-2 joins."


