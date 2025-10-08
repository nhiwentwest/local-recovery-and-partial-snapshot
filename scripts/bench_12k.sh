#!/usr/bin/env bash
set -euo pipefail

DURATION="${DURATION:-180}"
PROCS="${PROCS:-10}"
RPS_PER_PROC="${RPS_PER_PROC:-1200}"
LOAD_CMD="${LOAD_CMD:-./scripts/load-benchmark.sh}"

LOG="/tmp/bench_12k.$(date +%s).log"
echo "[bench] start ${DURATION}s ~$((${PROCS}*${RPS_PER_PROC})) rps" | tee -a "$LOG"

( KAFKA_NATIVE=true BROKERS_HOST=127.0.0.1:9092 TOPIC_IN=p1.orders GROUP=opa-g \
  DURATION="$DURATION" PROCS="$PROCS" RPS_PER_PROC="$RPS_PER_PROC" \
  "$LOAD_CMD" ) >"$LOG" 2>&1 &
LOAD_PID=$!

sleep 45
echo "[bench][$(date -Is)] KILL opb@1" | tee -a "$LOG"; pkill -f " -http :8089" || true
sleep 2
echo "[bench][$(date -Is)] START opb@1" | tee -a "$LOG"; nohup ./scripts/opb-start.sh >/tmp/opb1.out 2>&1 &

sleep 75
echo "[bench][$(date -Is)] KILL opb@2" | tee -a "$LOG"; pkill -f " -http :8090" || true
sleep 2
echo "[bench][$(date -Is)] START opb@2" | tee -a "$LOG"; (HTTP_ADDR=:8090 TX_ID=opb-tx-2 STATE_DIR=./data/opb2 ./scripts/opb-start.sh >/tmp/opb2.out 2>&1 &) 

wait "$LOAD_PID" || true
echo "[bench] done. log=$LOG" | tee -a "$LOG"


