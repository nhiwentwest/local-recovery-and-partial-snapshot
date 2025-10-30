#!/usr/bin/env bash
set -euo pipefail

# Crash-matrix runner for OpA (Exactly-Once) demo.
# Usage:
#   BOOTSTRAP=localhost:9092 TOPIC_IN=p1.orders TOPIC_OUT=p1.orders.enriched TX_ID=opa-eos-1 \
#   ./scripts/opa_crash_matrix.sh

: "${BOOTSTRAP:=localhost:9092}"
: "${GROUP_ID:=opa-test}"
: "${TOPIC_IN:=p1.orders}"
: "${TOPIC_OUT:=p1.orders.enriched}"
: "${TX_ID:=opa-crash-matrix}"
: "${HTTP_ADDR:=:8088}"

run_case() {
  local mode="$1" # before|mid|after|none
  echo "==== OpA crash-mode=$mode ===="
  set +e
  go run ./cmd/opa/main.go \
    -bootstrap "$BOOTSTRAP" \
    -group-id "$GROUP_ID" \
    -topic-in "$TOPIC_IN" \
    -topic-out "$TOPIC_OUT" \
    -tx-id "$TX_ID-$mode" \
    -crash-mode "$mode" \
    -http "$HTTP_ADDR"
  rc=$?
  set -e
  echo "OpA exited with code $rc (expected non-zero for before/mid/after)"
}

run_case before
run_case mid
run_case after
run_case none

echo "Done. Validate with a read_committed consumer that only committed messages are visible."


