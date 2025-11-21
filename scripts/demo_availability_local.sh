#!/usr/bin/env bash
set -euo pipefail

# Availability & Headroom Demo (local, single machine)
# Proves the system continues serving while a replica fails, and how a spare replica helps.

source scripts/setup_env.sh

# --- Configurable Env Vars ---
OPB1_HTTP=${OPB1_HTTP:-http://127.0.0.1:8089}
OPB2_HTTP=${OPB2_HTTP:-http://127.0.0.1:8090}
OPB3_HTTP=${OPB3_HTTP:-http://127.0.0.1:8091}
BIN_OPB=${BIN_OPB:-./bin/opb}
STORE=${STORE:-AVAIL-TEST}
PROBE_SEC=${PROBE_SEC:-2}
FAULT_LEN=${FAULT_LEN:-15}
RUN_LEN=${RUN_LEN:-45}
AUTO_Y=${AUTO_Y:-0}
INTERACTIVE=${INTERACTIVE:-1}

# --- Helper Functions ---
http_ok() { curl -sf "$1" >/dev/null 2>&1; }

countdown_continue() {
  local msg=${1:-"Continuing"}; local secs=${2:-5}
  for ((i=secs;i>0;i--)); do printf "\r%s (auto-continue in %ds)" "$msg" "$i"; sleep 1; done; echo
}

ask_continue() {
  local msg=${1:-"Press y to continue, n to abort"}
  if [[ "$AUTO_Y" == "1" ]]; then return 0; fi
  if [[ "$INTERACTIVE" == "0" ]]; then countdown_continue "$msg" 5; return 0; fi
  local ans=""; while true; do read -r -p "${msg} [y/n]: " ans || true; case "$ans" in y|Y) return 0;; n|N) return 1;; *) echo "Please answer y or n.";; esac; done
}

wait_ready() {
  local url=$1; local id=$2; local n=${3:-60}
  say "Waiting for $id ($url) to be healthy (up to ${n}s)..."
  for((i=0;i<n;i++)); do 
    if http_ok "$url"; then echo "OK"; return 0; fi; 
    sleep 1; 
    printf "."; 
  done; 
  echo "ERROR: Timeout waiting for $id.";
  tail -n 50 "./logs/${id}.out"
  return 1
}

start_instance(){
  local http=$1; local id=$2; local port=${http##*:}
  say "Starting instance $id on port $port..."
  OPB_PEERS="$OPB1_HTTP,$OPB2_HTTP,$OPB3_HTTP" \
  "$BIN_OPB" \
    --state-backend memory \
    --kafka-bootstrap "$BOOTSTRAP" --group-id opb-standalone \
    --input-source kafka \
    --topic-enriched p1.orders.enriched --output-topic p1.orders.output \
    --changelog-sink both --manifest-sink both \
    --topic-changelog p1.opb-changelog --topic-snapshots p1.opb-snapshots --topic-store-touch p1.opb-store-touch \
    --window-size 60 --tx-batch-size 1000 --tx-linger-ms 100 \
    --session-timeout-ms 6000 --heartbeat-interval-ms 2000 \
    --http :${port} --instance-id "$id" \
    > ./logs/${id}.out 2>&1 &
}

# --- Main Demo Logic ---

start_instance "$OPB1_HTTP" B1
wait_ready "$OPB1_HTTP/healthz" B1

start_instance "$OPB2_HTTP" B2
wait_ready "$OPB2_HTTP/healthz" B2

USE_SPARE=0
if ask_continue "Start a spare replica (B3) to demo N+1 headroom?"; then
  USE_SPARE=1
  start_instance "$OPB3_HTTP" B3
  wait_ready "$OPB3_HTTP/healthz" B3
fi

say "All instances are healthy. Starting background pump."
BG_PUMP_LOG=./logs/avail_pump.log
( while true; do 
    STORES="$STORE" N=400 BATCH=200 RANDOM_DELAY_MS_MIN=0 RANDOM_DELAY_MS_MAX=10 bash scripts/pump_random.sh >> "$BG_PUMP_LOG" 2>&1 || true;
    sleep 1;
  done ) &
PUMP_PID=$!

say "Probing cluster state every ${PROBE_SEC}s for ${RUN_LEN}s. Fault will be induced after ${FAULT_AT:-$((RUN_LEN/3))}s."
PROBE_LOG=./logs/avail_probe.log
: > "$PROBE_LOG"

FAULT_AT=$(( RUN_LEN / 3 ))
END_AT=$(( $(date +%s) + RUN_LEN ))
FAULTED=0

while true; do
  now=$(date +%s)
  if (( now >= END_AT )); then break; fi
  
  c1=$(curl -s -o /dev/null -w "%{http_code}" "$OPB1_HTTP/healthz" || echo 000)
  c2=$(curl -s -o /dev/null -w "%{http_code}" "$OPB2_HTTP/healthz" || echo 000)
  c3="n/a"
  if (( USE_SPARE == 1 )); then c3=$(curl -s -o /dev/null -w "%{http_code}" "$OPB3_HTTP/healthz" || echo 000); fi
  snap=$(curl -s "$OPB1_HTTP/api/cluster" | tr -d '\n')
  printf "[%s] health: B1=%s B2=%s B3=%s | cluster=%s\n" "$(date +%H:%M:%S)" "$c1" "$c2" "$c3" "$snap" >> "$PROBE_LOG"

  ran=$(( RUN_LEN - (END_AT - now) ))
  if (( FAULTED == 0 && ran >= FAULT_AT )); then
    say "Induce fault: killing B2 for ${FAULT_LEN}s"
    PIDS=$(pgrep -f "opb .*--instance-id B2" || true); if [ -n "$PIDS" ]; then kill $PIDS || true; fi
    FAULTED=1
    ( sleep "$FAULT_LEN"; start_instance "$OPB2_HTTP" B2; ) &
  fi
  
  sleep "$PROBE_SEC"
done

say "Stopping background pump"
kill "$PUMP_PID" || true

say "Summary (tail logs)"
TAIL_N=30
say "Probe log tail:"
tail -n "$TAIL_N" "$PROBE_LOG" || true

say "Open cluster overview: $OPB1_HTTP/viz/cluster"
