#!/bin/bash

# test_state_import.sh
# Verifies best-effort peer-assisted state import on rebalance.

set -euo pipefail

BASE_DIR=$(dirname "$0")/..
cd "$BASE_DIR"

mkdir -p ./logs

OPB_KAFKA_BOOTSTRAP="localhost:9092"
TOPIC_ENRICHED="p1.orders.enriched"
HTTP_A=":8095"
HTTP_B=":8096"
SNAP_DIR_A="./snapshots-import-A"
SNAP_DIR_B="./snapshots-import-B"
STATE_DIR_A="./data/opb-import-A"
STATE_DIR_B="./data/opb-import-B"
LOG_A="./logs/opb_import_A.log"
LOG_B="./logs/opb_import_B.log"
OPB_PID_A=""
OPB_PID_B=""

cleanup() {
  echo "Cleaning up..."
  pkill -f "opb -http ${HTTP_A}" || true
  pkill -f "opb -http ${HTTP_B}" || true
  if [[ -n "${OPB_PID_A}" ]] && kill -0 "${OPB_PID_A}" 2>/dev/null; then
    kill "${OPB_PID_A}" 2>/dev/null || true
    wait "${OPB_PID_A}" 2>/dev/null || true
  fi
  if [[ -n "${OPB_PID_B}" ]] && kill -0 "${OPB_PID_B}" 2>/dev/null; then
    kill "${OPB_PID_B}" 2>/dev/null || true
    wait "${OPB_PID_B}" 2>/dev/null || true
  fi
  rm -rf "$SNAP_DIR_A" "$SNAP_DIR_B" "$STATE_DIR_A" "$STATE_DIR_B"
}
trap 'ec=$?; if [[ $ec -ne 0 ]]; then echo "--- LOG A (last 200) ---"; tail -n 200 "$LOG_A" || true; echo "--- LOG B (last 200) ---"; tail -n 200 "$LOG_B" || true; fi; cleanup; exit $ec' EXIT

ensure_opb_bin() {
  if [[ -x ./bin/opb ]]; then
    echo "Using existing ./bin/opb"
  else
    echo "Building opb binary..."
    go build -o ./bin/opb ./cmd/opb
  fi
}

# 0. Ensure Kafka topics are present and clean
./scripts/run_infra.sh >/dev/null 2>&1 || ./scripts/run_infra.sh

ensure_opb_bin
OPB_BIN=./bin/opb

echo "Starting A (producer/consumer) ..."
"$OPB_BIN" -http "$HTTP_A" \
  -kafka-bootstrap "$OPB_KAFKA_BOOTSTRAP" \
  -changelog-sink "kafka" \
  -manifest-sink "file" \
  -snapshot-dir "$SNAP_DIR_A" \
  -state-dir "$STATE_DIR_A" \
  -input-source "kafka" \
  -topic-enriched "$TOPIC_ENRICHED" \
  -instance-id "A" \
  -restore-on-start=false &> "$LOG_A" &
OPB_PID_A=$!

# Wait for A healthy
for i in {1..60}; do
  if curl -s -f "http://localhost${HTTP_A}/healthz" >/dev/null; then
    echo "A healthy."
    break
  fi
  sleep 0.5
  if [[ $i -eq 60 ]]; then echo "A did not become healthy"; exit 1; fi
done

# 1. Inject some data to build state on A
STORE="TEST-IMP"
PROD="p1"
# Use a WS aligned to the default 300s window to match server keying
RAW_WS=1700000000
WS=$(( RAW_WS - (RAW_WS % 300) ))
curl -s -f -X POST "http://localhost${HTTP_A}/api/inject-test-data" \
  -H 'Content-Type: application/json' \
  -d "{\"storeId\":\"${STORE}\",\"productId\":\"${PROD}\",\"ws\":${WS},\"mode\":\"new\",\"n\":50,\"start\":0}" >/dev/null
# Give it some time to ingest
sleep 3

# 2. Start B with import enabled and peers=A
A_URL="http://127.0.0.1${HTTP_A}"
echo "Starting B (rebalance-import-state enabled) ..."
"$OPB_BIN" -http "$HTTP_B" \
  -kafka-bootstrap "$OPB_KAFKA_BOOTSTRAP" \
  -changelog-sink "kafka" \
  -manifest-sink "file" \
  -snapshot-dir "$SNAP_DIR_B" \
  -state-dir "$STATE_DIR_B" \
  -input-source "kafka" \
  -topic-enriched "$TOPIC_ENRICHED" \
  -instance-id "B" \
  -rebalance-import-state=true \
  -peers "$A_URL" \
  -restore-on-start=false &> "$LOG_B" &
OPB_PID_B=$!

# Wait for B healthy
for i in {1..60}; do
  if curl -s -f "http://localhost${HTTP_B}/healthz" >/dev/null; then
    echo "B healthy."
    break
  fi
  sleep 0.5
  if [[ $i -eq 60 ]]; then echo "B did not become healthy"; exit 1; fi
done

# Give extra time for rebalance and import logic to complete
echo "Waiting for state import to settle..."
sleep 5

# 3. Verify B has state for injected key (found=true)
URL_B_EXACT="http://localhost${HTTP_B}/api/exact?storeId=${STORE}&productId=${PROD}&ws=${WS}"
RESP=$(curl -s -f "$URL_B_EXACT")
FOUND=$(echo "$RESP" | jq -r '.found // false')
SUMQ=$(echo "$RESP" | jq -r '.sumQty // 0')
if [[ "$FOUND" != "true" ]]; then
  echo "FAIL: B did not have imported state. resp=$RESP"
  exit 1
fi
if [[ "$SUMQ" -lt 1 ]]; then
  echo "FAIL: B state sumQty invalid. resp=$RESP"
  exit 1
fi

echo "SUCCESS: State import test passed (found on B: sumQty=${SUMQ})."

cleanup

