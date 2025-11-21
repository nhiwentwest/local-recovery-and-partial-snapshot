#!/usr/bin/env bash
set -euo pipefail

# Start đầy đủ pipeline: OpA + OpB

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
HTTP_OPA=${HTTP_OPA:-:8088}
HTTP_OPB=${HTTP_OPB:-:8089}
WINDOW_SIZE=${WINDOW_SIZE:-1800}
STATE_BACKEND=${STATE_BACKEND:-memory}
LOG_OPA=${LOG_OPA:-/tmp/opa.log}
LOG_OPB=${LOG_OPB:-/tmp/opb.log}

say() { printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

# Kill cũ
say "Stopping old processes..."
pkill -f "bin/opa.*-http ${HTTP_OPA}" || true
pkill -f "bin/opb.*-http ${HTTP_OPB}" || true
sleep 1

# Build
if [ ! -f ./bin/opa ]; then
  say "Building OpA..."
  go build -o bin/opa ./cmd/opa
fi

if [ ! -f ./bin/opb ]; then
  say "Building OpB..."
  go build -o bin/opb ./cmd/opb
fi

# Start OpA
TX_ID_OPA="opa-$(date +%s)-$$"
say "Starting OpA (transaction ID: $TX_ID_OPA)..."
./bin/opa \
  -bootstrap "$BOOTSTRAP" \
  -group-id opa-pipeline \
  -topic-in p1.orders \
  -topic-out p1.orders.enriched \
  -tx-id "$TX_ID_OPA" \
  -crash-mode none \
  -http "$HTTP_OPA" \
  > "$LOG_OPA" 2>&1 &
OPA_PID=$!
say "OpA started (PID: $OPA_PID)"

# Đợi OpA ready
say "Waiting for OpA..."
for i in {1..15}; do
  if curl -s "http://localhost${HTTP_OPA/:/}/metrics" > /dev/null 2>&1; then
    say "OpA ready!"
    break
  fi
  sleep 1
done

# Start OpB (EOS enabled by default)
say "Starting OpB..."
./bin/opb \
  -kafka-bootstrap "$BOOTSTRAP" \
  -input-source kafka \
  -group-id "${GROUP_ID:-opb-standalone}" \
  -state-backend "$STATE_BACKEND" \
  -window-size "$WINDOW_SIZE" \
  -tx-linger-ms 20 \
  -tx-batch-size 10 \
  --instance-id "${INSTANCE_ID:-B1}" \
  -http "$HTTP_OPB" \
  --peers "${OPB_PEERS:-}" \
  > "$LOG_OPB" 2>&1 &
OPB_PID=$!
say "OpB started (PID: $OPB_PID)"

# Đợi OpB ready
say "Waiting for OpB..."
for i in {1..30}; do
  if curl -s "http://localhost${HTTP_OPB/:/}/healthz" > /dev/null 2>&1; then
    say "OpB ready!"
    break
  fi
  sleep 1
done

echo ""
echo "═══════════════════════════════════════"
echo "Pipeline started!"
echo "  OpA: http://localhost${HTTP_OPA/:/}/metrics"
echo "  OpB: http://localhost${HTTP_OPB/:/}/viz/"
echo ""
echo "To pump data (raw → enriched → heatmap):"
echo "  TOPIC=p1.orders MODE=raw N=10000 \\"
echo "    STORES=\"A-,B-,C-,D-,E-,F-,G-,H-,I-,J-\" \\"
echo "    PARALLEL=4 ./scripts/pump_random.sh"
echo ""
echo "Logs:"
echo "  OpA: tail -f $LOG_OPA"
echo "  OpB: tail -f $LOG_OPB"
echo "═══════════════════════════════════════"
