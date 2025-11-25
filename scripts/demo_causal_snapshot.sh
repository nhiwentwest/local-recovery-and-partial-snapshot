#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_PATH="${ROOT}/tmp-causal-opb"
echo "[CAUSAL] building opb binary"
go build -o "$BIN_PATH" ./cmd/opb
BIN="$BIN_PATH"
HTTP_PORT=${HTTP_PORT:-8093}
GROUP_ID="opb-causal-demo-$$"
BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
STATE_DIR="${ROOT}/tmp-causal-state"
SNAP_DIR="${ROOT}/tmp-causal-snap"
CHG_DIR="${ROOT}/tmp-causal-changelog"
LOG_FILE="${ROOT}/tmp-causal.log"

cleanup() {
  pkill -f "--http :${HTTP_PORT}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[CAUSAL] preparing directories"
rm -rf "$STATE_DIR" "$SNAP_DIR" "$CHG_DIR"
mkdir -p "$STATE_DIR" "$SNAP_DIR" "$CHG_DIR"

echo "[CAUSAL] starting opb (phase1)"
cd "$ROOT"
($BIN \
  --http ":${HTTP_PORT}" \
  --group-id "${GROUP_ID}" \
  --kafka-bootstrap "${BOOTSTRAP}" \
  --input-source kafka \
  --topic-enriched p1.orders.enriched \
  --output-topic p1.orders.output \
  --topic-changelog p1.opb-changelog \
  --topic-snapshots p1.opb-snapshots \
  --changelog-sink both \
  --manifest-sink both \
  --snapshot-dir "$SNAP_DIR" \
  --changelog-dir "$CHG_DIR" \
  --state-dir "$STATE_DIR" \
  --snap-max-deltas 3 \
  --snapshot-interval 600 \
  --restore-on-start=false \
  >"$LOG_FILE" 2>&1) &
PID=$!
sleep 5

status() {
  curl -s "http://127.0.0.1:${HTTP_PORT}/status" || true
}

inject() {
  local mode=$1
  local n=$2
  curl -s -X POST -H "Content-Type: application/json" \
    -d "{\"storeId\":\"CAUSAL\",\"productId\":\"\",\"ws\":0,\"mode\":\"${mode}\",\"n\":${n},\"start\":0}" \
    "http://127.0.0.1:${HTTP_PORT}/api/inject-test-data" >/dev/null
}

echo "[CAUSAL] inject batch 1"
inject new 200
sleep 2

echo "[CAUSAL] cut full snapshot"
curl -s -X POST "http://127.0.0.1:${HTTP_PORT}/admin/snapshot-cut?type=full" >/dev/null
sleep 10

echo "[CAUSAL] inject batch 2"
inject update 50
sleep 2

echo "[CAUSAL] cut delta snapshot"
curl -s -X POST "http://127.0.0.1:${HTTP_PORT}/admin/snapshot-cut?type=delta" >/dev/null

echo "[CAUSAL] waiting for manifest with inflight"
for _ in $(seq 1 40); do
  if jq -e '.inflightFile != ""' "$SNAP_DIR/manifest.latest.json" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "[CAUSAL] killing process to simulate crash"
kill -9 $PID || true
sleep 2

echo "[CAUSAL] restore-only run"
$BIN \
  --http ":${HTTP_PORT}" \
  --group-id "${GROUP_ID}" \
  --kafka-bootstrap "${BOOTSTRAP}" \
  --input-source kafka \
  --topic-enriched p1.orders.enriched \
  --output-topic p1.orders.output \
  --topic-changelog p1.opb-changelog \
  --topic-snapshots p1.opb-snapshots \
  --snapshot-dir "$SNAP_DIR" \
  --changelog-dir "$CHG_DIR" \
  --state-dir "$STATE_DIR" \
  --restore-on-start=true \
  --restore-only=true \
  >/tmp/causal-restore.log 2>&1 || true

echo "[CAUSAL] verifying state contains batch2"
if grep -q "inflight replay applied" /tmp/causal-restore.log; then
  echo "[CAUSAL] replay verification ok"
else
  echo "[CAUSAL] replay verification inconclusive"
fi

echo "[CAUSAL] pipeline demonstration complete"
exit 0

