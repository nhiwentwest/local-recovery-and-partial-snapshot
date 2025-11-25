#!/bin/bash

# test_chain_restore.sh
# Tests restoring state from a chain of snapshots (1 full + N deltas).

set -euo pipefail

BASE_DIR=$(dirname "$0")/..
cd "$BASE_DIR"

mkdir -p ./logs

OPB_KAFKA_BOOTSTRAP="localhost:9092"
OPB_HTTP_ADDR=":8089"
SNAPSHOT_DIR="./snapshots-chain-test"
STATE_DIR="./data/opb-chain-test"
LOG_FILE="./logs/opb_chain_setup.log"
OPB_PID=""

retry_until_200() {
  local method="$1"
  local url="$2"
  local data="${3:-}"
  local max_attempts="${4:-120}"
  local sleep_s="${5:-1}"

  local attempt=1
  local http_code
  local resp_file
  resp_file="$(mktemp)"

  while (( attempt <= max_attempts )); do
    if [[ -n "$data" ]]; then
      http_code=$(curl -sS -m 5 -X "$method" -H "Content-Type: application/json" \
        -d "$data" -o "$resp_file" -w "%{http_code}" "$url" || true)
    else
      http_code=$(curl -sS -m 5 -X "$method" -o "$resp_file" -w "%{http_code}" "$url" || true)
    fi

    if [[ "$http_code" == "200" ]]; then
      cat "$resp_file" || true
      rm -f "$resp_file"
      return 0
    fi

    echo "Waiting for $method $url to return 200 (got $http_code) attempt=$attempt/$max_attempts"
    sleep "$sleep_s"
    ((attempt++))
  done

  echo "ERROR: $method $url did not return 200 after $max_attempts attempts (last=$http_code)"
  [[ -s "$resp_file" ]] && { echo "--- Last response body ---"; tail -n +1 "$resp_file"; }
  rm -f "$resp_file"
  return 1
}

wait_manifest_exists() {
  local timeout=${1:-60}
  echo "Waiting for manifest file to appear at $SNAPSHOT_DIR/manifest.latest.json ..."
  for i in $(seq 1 "$timeout"); do
    if [[ -f "$SNAPSHOT_DIR/manifest.latest.json" ]]; then
      echo "Manifest file is present."
      return 0
    fi
    sleep 1
  done
  echo "ERROR: manifest.latest.json not found after ${timeout}s"
  return 1
}

cleanup() {
  echo "Cleaning up..."
  pkill -f "opb -http ${OPB_HTTP_ADDR}" || true
  if [[ -n "${OPB_PID}" ]] && kill -0 "${OPB_PID}" 2>/dev/null; then
    kill "${OPB_PID}" 2>/dev/null || true
    wait "${OPB_PID}" 2>/dev/null || true
  fi
  rm -rf "$SNAPSHOT_DIR" "$STATE_DIR" "./data/opb-chain-test-restored" "./logs/opb_chain_restore_run.log"
}

trap 'ec=$?; if [[ $ec -ne 0 ]]; then echo "--- OPB LOG (last 400 lines) ---"; tail -n 400 "$LOG_FILE" || true; fi; cleanup; exit $ec' EXIT

# Build or use prebuilt opb to avoid slow `go run`
ensure_opb_bin() {
  if [[ -x ./bin/opb ]]; then
    echo "Using existing ./bin/opb"
    OPB_BIN=./bin/opb
  else
    echo "Building opb binary..."
    go build -o ./bin/opb ./cmd/opb
    OPB_BIN=./bin/opb
  fi
}

ensure_opb_bin

# 1. Start opb
echo "Starting opb to create snapshots..."
"$OPB_BIN" -http "$OPB_HTTP_ADDR" \
  -kafka-bootstrap "$OPB_KAFKA_BOOTSTRAP" \
  -changelog-sink "kafka" \
  -manifest-sink "file" \
  -snapshot-dir "$SNAPSHOT_DIR" \
  -state-dir "$STATE_DIR" \
  -snap-max-deltas 10 \
  -input-source "kafka" \
  -restore-on-start=false &> "$LOG_FILE" &
OPB_PID=$!

# Wait for opb to be ready (best-effort healthz)
echo "Waiting for opb to be healthy..."
for i in {1..40}; do
  if curl -s -f "http://localhost:8089/healthz" > /dev/null; then
    echo "opb is healthy."
    break
  fi
  sleep 0.5
done

# 2. Create a chain of snapshots
# Treat the first full snapshot as the true readiness gate.
echo "Creating snapshot chain (1 full, 3 deltas)..."
retry_until_200 POST "http://localhost:8089/admin/snapshot-cut?type=full" "" 180 1
sleep 3
retry_until_200 POST "http://localhost:8089/admin/snapshot-cut?type=delta" "" 60 1
sleep 3
retry_until_200 POST "http://localhost:8089/admin/snapshot-cut?type=delta" "" 60 1
sleep 3
retry_until_200 POST "http://localhost:8089/admin/snapshot-cut?type=delta" "" 60 1
sleep 3

# Ensure manifest file exists before stopping opb
wait_manifest_exists 60

# 3. Stop opb
echo "Stopping opb..."
pkill -f "opb -http ${OPB_HTTP_ADDR}" || true
sleep 1 # Give it a moment to shut down

# 4. Restore from the chain
echo "Restoring from chain..."
RESTORE_LOG="./logs/opb_chain_restore_run.log"

"$OPB_BIN" -http ":8090" \
  -kafka-bootstrap "$OPB_KAFKA_BOOTSTRAP" \
  -manifest-source "file" \
  -snapshot-dir "$SNAPSHOT_DIR" \
  -state-dir "./data/opb-chain-test-restored" \
  -restore-on-start=true \
  -restore-only=true &> "$RESTORE_LOG"

# 5. Verify the restore log
echo "Verifying restore log..."
if grep -q "restore-only: exiting after successful restore" "$RESTORE_LOG" || grep -q "restore completed:" "$RESTORE_LOG"; then
  echo "SUCCESS: Chain restore test passed."
else
  echo "FAIL: Restore log did not show a successful restore."
  tail -n 400 "$RESTORE_LOG" || true
  exit 1
fi
