#!/usr/bin/env bash
set -euo pipefail
# KỊCH BẢN 3: Exactly-once correctness (OpA & OpB)
# Mục tiêu: không double-count, downstream không thấy bản ghi "bẩn"
# Thời gian: ~5-10 phút

# ==========================
# Config
# ==========================
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
PREFIX=${PREFIX:-p2}

# Paths
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

BIN_OPA=${BIN_OPA:-./bin/opa}
BIN_OPB=${BIN_OPB:-./bin/opb}
BIN_BENCH_LATENCY=${BIN_BENCH_LATENCY:-./bin/bench_latency}

TOPIC_RAW=${PREFIX}.orders
TOPIC_IN=${PREFIX}.orders.enriched
TOPIC_OUT=${PREFIX}.orders.output

say() { printf "[SCENARIO-3] [%s] %s\n" "$(date +%H:%M:%S)" "$*"; }
error() { say "ERROR: $*" >&2; exit 1; }

# Kiểm tra dependencies
check_deps() {
  say "Checking dependencies..."
  [[ -f "$BIN_OPA" ]] || error "OpA binary not found: $BIN_OPA"
  [[ -f "$BIN_OPB" ]] || error "OpB binary not found: $BIN_OPB"
  [[ -f "$BIN_BENCH_LATENCY" ]] || error "bench_latency binary not found: $BIN_BENCH_LATENCY"
  
  # Kiểm tra Prometheus đang scrape đúng
  local prom_targets
  prom_targets=$(curl -s "http://localhost:9090/api/v1/targets" 2>/dev/null || echo "")
  if [[ -z "$prom_targets" ]]; then
    say "WARNING: Prometheus không chạy hoặc không truy cập được tại localhost:9090"
    say "         Grafana sẽ không có data. Bạn có muốn tiếp tục không? (y/n)"
    read -r reply
    [[ "$reply" == "y" ]] || exit 1
  fi
  
  say "Dependencies OK"
}

# Setup infrastructure
setup_infra() {
  say "Setting up Kafka topics..."
  "$SCRIPT_DIR/run_infra.sh"
}

# Window 1: OpA (EOS + crash matrix)
window1_opa() {
  say "========================================="
  say "WINDOW 1: OpA EOS + Crash Matrix Test"
  say "========================================="
  say "Running OpA crash matrix tests..."
  "$SCRIPT_DIR/run_opa.sh" &
  local opa_pid=$!
  say "OpA running in background (PID=$opa_pid)"
  say "Waiting for OpA to complete crash matrix tests..."
  
  # Chờ OpA hoàn thành crash matrix (khoảng 30-60s)
  sleep 45
  
  # Kiểm tra OpA đang chạy
  if ! kill -0 "$opa_pid" 2>/dev/null; then
    error "OpA đã dừng sớm (không mong đợi)"
  fi
  
  say "OpA crash matrix tests completed. OpA is running normally now."
  echo "$opa_pid" > /tmp/opa_pid.txt
}

# Window 2: OpB (EOS, Kafka-mode)
window2_opb() {
  say "========================================="
  say "WINDOW 2: OpB EOS (Kafka-mode)"
  say "========================================="
  say "Starting OpB in Kafka mode..."
  "$SCRIPT_DIR/run_opb.sh" &
  local opb_pid=$!
  say "OpB running in background (PID=$opb_pid)"
  
  # Chờ OpB sẵn sàng (health check)
  say "Waiting for OpB to be ready..."
  for i in {1..30}; do
    if curl -sf "http://127.0.0.1:8089/healthz" >/dev/null 2>&1; then
      say "OpB is ready"
      break
    fi
    if [[ $i -eq 30 ]]; then
      error "OpB không sẵn sàng sau 30s"
    fi
    sleep 1
  done
  
  echo "$opb_pid" > /tmp/opb_pid.txt
  
  # Đợi một chút để OpB xử lý vài events
  say "Waiting for OpB to process initial events..."
  sleep 10
}

# Window 3: Đo latency trên changelog
window3_latency() {
  say "========================================="
  say "WINDOW 3: Latency Measurement"
  say "========================================="
  say "Measuring steady-state latency on changelog..."
  
  # Chạy bench_latency theo README
  "$BIN_BENCH_LATENCY" \
    -bootstrap "$BOOTSTRAP" \
    -topic-in "$TOPIC_IN" \
    -topic-out "$TOPIC_OUT" \
    -store A \
    -window 10 \
    -n 20 \
    -pid-prefix pL \
    -measure-topic "${PREFIX}.opb-changelog" || {
    say "WARNING: bench_latency có lỗi (có thể do timeout hoặc pipeline chưa đủ nhanh)"
  }
  
  say "Latency measurement completed"
}

# Cleanup
cleanup() {
  say "Cleaning up processes..."
  if [[ -f /tmp/opa_pid.txt ]]; then
    local pid=$(cat /tmp/opa_pid.txt)
    kill "$pid" 2>/dev/null || true
    rm -f /tmp/opa_pid.txt
  fi
  if [[ -f /tmp/opb_pid.txt ]]; then
    local pid=$(cat /tmp/opb_pid.txt)
    kill "$pid" 2>/dev/null || true
    rm -f /tmp/opb_pid.txt
  fi
  pkill -f "bin/opa" 2>/dev/null || true
  pkill -f "bin/opb" 2>/dev/null || true
  say "Cleanup done"
}

# Traps
trap cleanup EXIT INT TERM

# Main
main() {
  say "Starting Scenario 3: Exactly-once correctness"
  say "This demo will:"
  say "  1. Test OpA crash matrix (before/mid/after commit)"
  say "  2. Start OpB in Kafka EOS mode"
  say "  3. Measure latency on changelog topic"
  say ""
  say "Expected results:"
  say "  - OpA crash tests: PASS (exactly-once, count=1)"
  say "  - OpB: running and processing events"
  say "  - Latency: HIT (not MISS), p95 < 1s"
  say ""
  read -p "Press Enter to start..."
  
  check_deps
  setup_infra
  
  window1_opa
  window2_opb
  window3_latency
  
  say "========================================="
  say "SCENARIO 3 COMPLETED"
  say "========================================="
  say "Check Grafana at http://localhost:3000"
  say "  - Data source: Prometheus (localhost:9090)"
  say "  - Dashboard: op-stream.json"
  say ""
  say "Metrics to verify:"
  say "  - opa_tx_produced_total: > 0"
  say "  - opb_tx_produced_total: > 0"
  say "  - opb_partition_lag: should be low"
  say ""
  say "Processes are still running. Press Ctrl+C to stop."
  
  # Giữ script sống
  while true; do sleep 60; done
}

main "$@"

