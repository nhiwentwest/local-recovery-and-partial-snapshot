#!/usr/bin/env bash
set -euo pipefail

# One-shot EOS verification scenario for OpA with Kafka (no Docker required).
# It will:
# 1) Start an OpA instance with crash-mode=none (background) and pump baseline data
# 2) Run three crash cases (before/mid/after) with separate group-ids
# 3) After each step, use cmd/verify_eos to compare read_committed vs read_uncommitted

: "${BOOTSTRAP:=localhost:9092}"
: "${TOPIC_IN:=p1.orders}"
: "${TOPIC_OUT:=p1.orders.enriched}"
: "${DURATION:=5}"

say() { printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

verify() {
  ./verify_eos -bootstrap "$BOOTSTRAP" -topic "$TOPIC_OUT" -duration "$DURATION" || go run ./cmd/verify_eos -bootstrap "$BOOTSTRAP" -topic "$TOPIC_OUT" -duration "$DURATION"
}

pump() {
  local n=$1
  BOOTSTRAP="$BOOTSTRAP" TOPIC="$TOPIC_IN" N="$n" CHUNK="$n" SLEEP=0 MODE=raw PARALLEL=1 ./scripts/pump_test.sh
}

cleanup_bg() {
  if [[ -n "${OPA_BG_PID:-}" ]]; then
    kill "$OPA_BG_PID" 2>/dev/null || true
  fi
}
trap cleanup_bg EXIT

say "Start OpA (none) in background and pump baseline..."
nohup go run ./cmd/opa/main.go \
  -bootstrap "$BOOTSTRAP" -group-id opa-none-verify \
  -topic-in "$TOPIC_IN" -topic-out "$TOPIC_OUT" \
  -tx-id opa-none-verify -crash-mode none >/tmp/opa_none_verify.log 2>&1 &
OPA_BG_PID=$!
sleep 1

pump 300
say "Baseline verify:"
verify

say "Case BEFORE (expect uncommitted > committed)"
nohup go run ./cmd/opa/main.go \
  -bootstrap "$BOOTSTRAP" -group-id opa-before-verify \
  -topic-in "$TOPIC_IN" -topic-out "$TOPIC_OUT" \
  -tx-id opa-before-verify -crash-mode before >/tmp/opa_before_verify.log 2>&1 &
sleep 1
pump 50
verify

say "Case MID (expect uncommitted > committed)"
nohup go run ./cmd/opa/main.go \
  -bootstrap "$BOOTSTRAP" -group-id opa-mid-verify \
  -topic-in "$TOPIC_IN" -topic-out "$TOPIC_OUT" \
  -tx-id opa-mid-verify -crash-mode mid >/tmp/opa_mid_verify.log 2>&1 &
sleep 1
pump 50
verify

say "Case AFTER (expect both increase; committed visible)"
nohup go run ./cmd/opa/main.go \
  -bootstrap "$BOOTSTRAP" -group-id opa-after-verify \
  -topic-in "$TOPIC_IN" -topic-out "$TOPIC_OUT" \
  -tx-id opa-after-verify -crash-mode after >/tmp/opa_after_verify.log 2>&1 &
sleep 1
pump 50
verify

say "Done. Check above lines for committed vs uncommitted counts."


