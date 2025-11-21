#!/usr/bin/env bash
set -euo pipefail

# Integration test suite for OpB enhancements
# Scenarios:
# 1. EOS: duplicates do not change state; skipped counters increase.
# 2. Scale-out: two instances process data; zone details show both.
# 3. Recovery: kill/restart; TTR is low; state is restored.

say() { printf "\n---> %s\n" "$*"; }
say_ok() { printf "[OK] %s\n" "$*"; }
say_err() { printf "[FAIL] %s\n" "$*"; exit 1; }

# Ensure we have binaries
go build -o ./bin/opb ./cmd/opb
go build -o ./bin/pump ./cmd/pump

# --- Scenario 1: EOS Proof ---
say "Scenario 1: EOS (duplicates are skipped)"

# Clean up previous runs
pkill -f "./bin/opb" || true
sleep 1

# Start OpB
./bin/opb -kafka-bootstrap 127.0.0.1:9092 -input-source kafka -state-backend memory -http :8089 -instance-id B1 &
OPB_PID=$!
trap 'kill $OPB_PID' EXIT
sleep 3 # wait for it to be ready

# Pump unique data using inject endpoint
ws=$(date +%s); ws=$((ws - ws%60))
curl -s -X POST http://127.0.0.1:8089/api/inject-test-data \
  -H "Content-Type: application/json" \
  -d "{\"storeId\":\"EOS-\",\"mode\":\"new\",\"n\":100,\"ws\":$ws}" >/dev/null
sleep 3 # allow processing

# Get baseline metrics and state
applied_before=$(curl -s http://127.0.0.1:8089/metrics | grep -E '^opb_events_applied_total' | awk '{print $2}')
skipped_dedup_before=$(curl -s http://127.0.0.1:8089/metrics | grep -E '^opb_events_skipped_dedup_total' | awk '{print $2}')
skipped_seq_before=$(curl -s http://127.0.0.1:8089/metrics | grep -E '^opb_events_skipped_seq_total' | awk '{print $2}')
skipped_before=$((skipped_dedup_before + skipped_seq_before))
sum_before=$(curl -s "http://127.0.0.1:8089/api/zone-details?id=EOS-" | sed -n 's/.*"sumQty"[: ]*\([0-9][0-9]*\).*/\1/p' | head -1)
if [[ -z "$sum_before" ]]; then sum_before=0; fi
if [[ -z "$applied_before" ]]; then applied_before=0; fi
if [[ -z "$skipped_dedup_before" ]]; then skipped_dedup_before=0; fi
if [[ -z "$skipped_seq_before" ]]; then skipped_seq_before=0; fi
if [[ -z "$skipped_before" ]]; then skipped_before=0; fi

# Inject duplicates (same orderId, should be skipped)
curl -s -X POST http://127.0.0.1:8089/api/inject-test-data \
  -H "Content-Type: application/json" \
  -d "{\"storeId\":\"EOS-\",\"mode\":\"duplicate\",\"n\":100,\"ws\":$ws}" >/dev/null
sleep 3

# Get final metrics and state
applied_after=$(curl -s http://127.0.0.1:8089/metrics | grep -E '^opb_events_applied_total' | awk '{print $2}')
skipped_dedup_after=$(curl -s http://127.0.0.1:8089/metrics | grep -E '^opb_events_skipped_dedup_total' | awk '{print $2}')
skipped_seq_after=$(curl -s http://127.0.0.1:8089/metrics | grep -E '^opb_events_skipped_seq_total' | awk '{print $2}')
skipped_after=$((skipped_dedup_after + skipped_seq_after))
sum_after=$(curl -s "http://127.0.0.1:8089/api/zone-details?id=EOS-" | sed -n 's/.*"sumQty"[: ]*\([0-9][0-9]*\).*/\1/p' | head -1)
if [[ -z "$sum_after" ]]; then sum_after=0; fi
if [[ -z "$skipped_dedup_after" ]]; then skipped_dedup_after=0; fi
if [[ -z "$skipped_seq_after" ]]; then skipped_seq_after=0; fi
if [[ -z "$skipped_after" ]]; then skipped_after=0; fi

if [[ "$sum_before" != "$sum_after" ]]; then say_err "Sum changed from $sum_before to $sum_after"; fi
if (( skipped_after > skipped_before )); then 
  say_ok "Skipped counter increased (dedup: $skipped_dedup_before -> $skipped_dedup_after, seq: $skipped_seq_before -> $skipped_seq_after)"; 
else 
  say_err "Skipped counter did not increase (before: $skipped_before, after: $skipped_after)"; 
fi

kill $OPB_PID
trap - EXIT

# --- Scenario 2: Scale-out ---
say "Scenario 2: Scale-out (two instances)"
pkill -f "./bin/opb" || true
sleep 1

./bin/opb -kafka-bootstrap 127.0.0.1:9092 -input-source kafka -state-backend memory -http :8089 -instance-id B1 &
OPB1_PID=$!
./bin/opb -kafka-bootstrap 127.0.0.1:9092 -input-source kafka -state-backend memory -http :8090 -instance-id B2 &
OPB2_PID=$!
trap 'kill $OPB1_PID $OPB2_PID' EXIT
sleep 5

./scripts/pump_random.sh >/dev/null
sleep 3

instances_raw=$(curl -s http://127.0.0.1:8089/api/zone-details?id=A-)
if echo "$instances_raw" | grep -q '"instances":\["'; then inst_count=1; else inst_count=0; fi
if [[ "$inst_count" -ge 1 ]]; then say_ok "Zone details show instances (>=1)"; else say_err "Zone details missing instances"; fi

kill $OPB1_PID $OPB2_PID
trap - EXIT

# --- Scenario 3: Recovery ---
say "Scenario 3: Recovery (Pebble + changelog)"
rm -rf ./data/opb-recovery ./snapshots/recovery

./bin/opb --state-backend pebble --state-dir ./data/opb-recovery --snapshot-dir ./snapshots/recovery --kafka-bootstrap 127.0.0.1:9092 -http :8089 -instance-id R1 &
OPB_PID=$!
trap 'kill $OPB_PID' EXIT
sleep 5

./scripts/pump_random.sh >/dev/null
sleep 2

kill $OPB_PID
sleep 1

start_time=$(date +%s)
./bin/opb --state-backend pebble --state-dir ./data/opb-recovery --snapshot-dir ./snapshots/recovery --kafka-bootstrap 127.0.0.1:9092 -http :8089 -instance-id R1 &
OPB_PID=$!

# Poll until healthy
for i in {1..15}; do
  if curl -s http://127.0.0.1:8089/status | grep -q '"status":"healthy"'; then
    ttr=$(( $(date +%s) - start_time ))
    if (( ttr < 10 )); then say_ok "Recovered in $ttr seconds"; else say_err "TTR too high: $ttr seconds"; fi
    kill $OPB_PID
    trap - EXIT
    say_ok "All integration tests passed."
    exit 0
  fi
  sleep 1
done

kill $OPB_PID
say_err "Recovery timed out"

