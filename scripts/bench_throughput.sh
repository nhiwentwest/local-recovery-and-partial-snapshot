#!/usr/bin/env bash
set -euo pipefail

# --- Config ---
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
PROM_HTTP=${PROM_HTTP:-http://127.0.0.1:9090}
PREFIX=${PREFIX:-p1}
NUM_OPB=${NUM_OPB:-4}
DATA_PARTITIONS=${DATA_PARTITIONS:-16}
OPA_HTTP=${OPA_HTTP:-:8088}
OPB_BASE_PORT=${OPB_BASE_PORT:-8089}

# Binaries & Logs
BIN_OPA=./bin/opa
BIN_OPB=./bin/opb
BIN_KADMIN=./bin/kadmin
LOG_DIR=/tmp
STATE_DIR_BASE=./data/opb-bench

# Pump settings
N=${N:-500000}
PARALLEL=${PARALLEL:-8}
BATCH=${BATCH:-5000}

say() { printf "\n\e[1;36m[BENCH]\e[0m %s\n" "$*"; }

# --- Stop all previous processes ---
say "Stopping all previous benchmark processes..."
pkill -f "bin/opa.*-group-id opa-bench" >/dev/null 2>&1 || true
pkill -f "bin/opb.*-group-id opb-bench" >/dev/null 2>&1 || true
sleep 2

# --- Build ---
go build -o "$BIN_KADMIN" ./cmd/kadmin
go build -o "$BIN_OPA" ./cmd/opa
go build -o "$BIN_OPB" ./cmd/opb

# --- Reset Kafka ---
say "Resetting Kafka topics..."
ENRICHED_PARTITIONS="$DATA_PARTITIONS" OUTPUT_PARTITIONS="$DATA_PARTITIONS" \
PREFIX="$PREFIX" BOOTSTRAP="$BOOTSTRAP" bash scripts/run_infra.sh

# --- Start Services ---

# OpA
say "Starting OpA..."
nohup "$BIN_OPA" -bootstrap "$BOOTSTRAP" -group-id opa-bench -topic-in "${PREFIX}.orders" \
  -topic-out "${PREFIX}.orders.enriched" -tx-id "opa-bench-tx" -http "$OPA_HTTP" \
  > "${LOG_DIR}/opa_bench.log" 2>&1 &
sleep 5

# OpB Fleet
say "Starting ${NUM_OPB} OpB instances..."
for ((i=0;i<NUM_OPB;i++)); do
  inst="B$((i+1))"
  http=":$(($OPB_BASE_PORT + i))"
  state_dir="${STATE_DIR_BASE}-${inst}"
  rm -rf "$state_dir"; mkdir -p "$state_dir"
  nohup "$BIN_OPB" --kafka-bootstrap "$BOOTSTRAP" --input-source kafka --group-id opb-bench \
    --topic-enriched "${PREFIX}.orders.enriched" --state-backend pebble --state-dir "$state_dir" \
    --instance-id "$inst" --http "$http" > "${LOG_DIR}/opb_${inst}.log" 2>&1 &
done

say "Waiting 15s for all instances to start and stabilize..."
sleep 15

# --- Pump Data ---
say "Pumping ${N} events..."
TOPIC="${PREFIX}.orders.enriched" N="$N" PARALLEL="$PARALLEL" BATCH="$BATCH" \
bash scripts/pump_random.sh

# --- Measure & Report ---
say "Pump finished. Waiting 15s for metrics to propagate..."
sleep 15

say "Querying Prometheus for throughput..."
TPUT=$(curl -s "${PROM_HTTP}/api/v1/query?query=sum(increase(opb_events_applied_total%5B30s%5D))%2F30%20or%20on()%20vector(0)" | jq -r '.data.result[0].value[1]')

echo
say "===================================="
say "Average Throughput: ${TPUT} events/s"
say "===================================="
echo
say "The system is still running. When finished, run './scripts/stop_bench.sh' to clean up."
