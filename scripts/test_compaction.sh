#!/bin/bash

# test_compaction.sh
# Tests the snapshot compaction policy (--snap-max-deltas).

set -euo pipefail

BASE_DIR=$(dirname "$0")/..
cd "$BASE_DIR"

mkdir -p ./logs

OPB_KAFKA_BOOTSTRAP="localhost:9092"
OPB_HTTP_ADDR=":8088"
SNAPSHOT_DIR="./snapshots-compaction-test"
LOG_FILE="./logs/opb_compaction_test.log"
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

cleanup() {
  echo "Cleaning up..."
  if [[ -n "${OPB_PID}" ]] && kill -0 "${OPB_PID}" 2>/dev/null; then
    kill "${OPB_PID}" 2>/dev/null || true
    wait "${OPB_PID}" 2>/dev/null || true
  fi
  rm -rf "$SNAPSHOT_DIR"
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

# 1. Start opb with a small delta limit
echo "Starting opb with --snap-max-deltas=2..."
"$OPB_BIN" -http "$OPB_HTTP_ADDR" \
  -kafka-bootstrap "$OPB_KAFKA_BOOTSTRAP" \
  -changelog-sink "kafka" \
  -manifest-sink "file" \
  -snapshot-dir "$SNAPSHOT_DIR" \
  -snap-max-deltas 2 \
  -input-source "kafka" \
  -restore-on-start=false &> "$LOG_FILE" &
OPB_PID=$!

# Wait for opb to be ready by polling health check (best-effort)
echo "Waiting for opb to be healthy..."
for i in {1..40}; do
  if curl -s -f "http://localhost:8088/healthz" > /dev/null; then
    echo "opb is healthy."
    break
  fi
  sleep 0.5
done

# 2. Cut initial full snapshot (treat as readiness gate)
echo "Cutting initial full snapshot..."
retry_until_200 POST "http://localhost:8088/admin/snapshot-cut?type=full" "" 180 1
sleep 3 # Wait for snapshot to be written

# 3. Cut delta 1
echo "Cutting delta 1..."
curl -s -f -X POST "http://localhost:8088/admin/snapshot-cut?type=auto" > /dev/null
sleep 3

# 4. Cut delta 2
echo "Cutting delta 2..."
curl -s -f -X POST "http://localhost:8088/admin/snapshot-cut?type=auto" > /dev/null
sleep 3

# 5. Cut next snapshot, which should trigger compaction to full
echo "Cutting next snapshot (expecting compaction to full)..."
curl -s -f -X POST "http://localhost:8088/admin/snapshot-cut?type=auto" > /dev/null
sleep 3

# 6. Verify the latest manifest
echo "Verifying latest manifest..."
LATEST_MANIFEST="$SNAPSHOT_DIR/manifest.latest.json"

if ! jq -e '.snapshotType == "full"' "$LATEST_MANIFEST"; then
  echo "FAIL: Expected latest snapshot to be 'full' due to compaction policy."
  cat "$LATEST_MANIFEST"
  exit 1
fi

if ! jq -e '(.deltaSequence // 0) == 0' "$LATEST_MANIFEST"; then
    echo "FAIL: Expected deltaSequence to be 0 for a new full snapshot."
    cat "$LATEST_MANIFEST"
    exit 1
fi

echo "SUCCESS: Compaction policy test passed."
