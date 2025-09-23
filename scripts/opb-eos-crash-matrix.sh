#!/usr/bin/env bash
set -euo pipefail

# Automated EOS crash-matrix for OpB (transactional output)
# - Runs 3 cases: before, mid, after commit
# - Kills OpB at controlled timing and asserts outcomes using read_committed

BROKERS="localhost:19092"
BROKERS_DOCKER="redpanda:9092"
TOPIC_ENRICHED="p2.orders.enriched"
TOPIC_OUTPUT="p2.orders.output"
GROUP_ID="opb-g1"
WINDOW_SIZE=60

require() { command -v "$1" >/dev/null 2>&1 || { echo "missing command: $1" >&2; exit 1; }; }
require docker

ensure_bin() {
  if [[ ! -x bin/opb ]]; then
    echo "[build] bin/opb"
    go build -o bin/opb ./cmd/opb
  fi
}

consume_output_tail() {
  docker exec redpanda rpk topic consume "$TOPIC_OUTPUT" \
    --brokers "$BROKERS_DOCKER" -o end --read-committed -n 50 --format '%k %v' || true
}

run_case() {
  local name="$1"; shift
  local sleep_sec="$1"; shift

  local ts=1694500200
  local store="A"
  local product="p${name}"
  local key="${store}#${product}#${ts}"
  local txid="opb-local-${name}-$(date +%s%N)"

  ensure_bin

  echo "\n=== CASE: ${name} (sleep ${sleep_sec}s before kill) ==="

  # Start OpB (EOS) in background
  ./bin/opb \
    --kafka-bootstrap "$BROKERS" \
    --input-source kafka \
    --group-id "$GROUP_ID" \
    --topic-enriched "$TOPIC_ENRICHED" \
    --output-topic "$TOPIC_OUTPUT" \
    --output-tx-id "$txid" \
    --window-size "$WINDOW_SIZE" &
  local opb_pid=$!

  # Give OpB a moment to start
  sleep 0.3

  # Produce one enriched event (with normTs)
  docker exec redpanda bash -lc "cat <<'JSON' | rpk topic produce $TOPIC_ENRICHED --brokers $BROKERS_DOCKER
{\"orderId\":\"o-${name}\",\"productId\":\"${product}\",\"storeId\":\"${store}\",\"price\":10000,\"qty\":1,\"ts\":${ts},\"normTs\":${ts},\"validated\":true}
JSON" >/dev/null

  # Kill at requested timing
  sleep "$sleep_sec"
  kill -9 "$opb_pid" 2>/dev/null || true
  wait "$opb_pid" 2>/dev/null || true

  # Small settle
  sleep 0.5

  # Inspect committed output tail and count this key
  local tail
  tail=$(consume_output_tail)
  local count
  count=$(printf "%s" "$tail" | grep -c "^${key} ") || true

  echo "[assert] key=${key} occurrences=${count}"
  case "$name" in
    before)
      if [[ "$count" -ne 0 ]]; then
        echo "[FAIL] 'before' should not commit any record for key=$key" >&2
        exit 1
      fi
      ;;
    mid)
      if [[ "$count" -gt 1 ]]; then
        echo "[FAIL] 'mid' should not double-commit; found $count for key=$key" >&2
        exit 1
      fi
      ;;
    after)
      if [[ "$count" -lt 1 ]]; then
        echo "[FAIL] 'after' should have exactly one committed record for key=$key" >&2
        exit 1
      fi
      if [[ "$count" -gt 1 ]]; then
        echo "[FAIL] 'after' should not double-commit; found $count for key=$key" >&2
        exit 1
      fi
      ;;
  esac

  echo "[OK] ${name}"
}

main() {
  echo "[info] Using brokers: host=$BROKERS docker=$BROKERS_DOCKER"
  echo "[info] Topics: enriched=$TOPIC_ENRICHED output=$TOPIC_OUTPUT"

  # Run three cases: tune sleep for your environment
  run_case before 0.05
  run_case mid    0.35
  run_case after  1.50

  echo "\nAll cases passed."
}

main "$@"


