#!/usr/bin/env bash
set -euo pipefail

# Scale-out demo with y/n prompts (or timed auto-continue).
# Shows how a second OpB instance (B2) joins the same group and updates the same store,
# and the web /viz/zone-data?id=STORE shows instances [B1, B2].
#
# Env flags:
# - AUTO_Y=1       -> always continue without prompts
# - INTERACTIVE=0  -> show short countdown then continue (for IDEs without TTY)
#
# Tunables:
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
OPB1_HTTP=${OPB1_HTTP:-http://127.0.0.1:8089}
OPB2_HTTP=${OPB2_HTTP:-http://127.0.0.1:8090}
GROUP_ID=${GROUP_ID:-opb-standalone}
STATE2_DIR=${STATE2_DIR:-./data/opb-standalone-2}
SNAPSHOT_DIR=${SNAPSHOT_DIR:-./snapshots}
INSTANCE2_ID=${INSTANCE2_ID:-B2}
BIN_OPB=${BIN_OPB:-./bin/opb}
STORE=${STORE:-EOS-TEST-D-}
PUMP_N=${PUMP_N:-1500}
PUMP_BATCH=${PUMP_BATCH:-300}
AUTO_Y=${AUTO_Y:-0}
INTERACTIVE=${INTERACTIVE:-1}

# If interactive prompts are desired but stdin isn't a TTY, bind stdin to /dev/tty
if [[ "$AUTO_Y" != "1" && "$INTERACTIVE" != "0" && ! -t 0 && -e /dev/tty ]]; then
  exec </dev/tty
fi

say() { printf "\n\e[1;36m[SCALE-OUT]\e[0m %s\n" "$*"; }
http_ok() { curl -sf "$1" >/dev/null 2>&1; }
countdown_continue() {
  local msg=${1:-"Continuing"}
  local secs=${2:-5}
  for ((i=secs;i>0;i--)); do printf "\r%s (auto-continue in %ds)" "$msg" "$i"; sleep 1; done; echo
}
ask_continue() {
  local msg=${1:-"Press y to continue, n to abort"}
  if [[ "$AUTO_Y" == "1" ]]; then return 0; fi
  if [[ "$INTERACTIVE" == "0" ]]; then countdown_continue "$msg" 5; return 0; fi
  local ans=""
  while true; do
    read -r -p "${msg} [y/n]: " ans || true
    case "$ans" in
      y|Y) return 0 ;;
      n|N) return 1 ;;
      *) echo "Please answer y or n." ;;
    esac
  done
}

wait_for_http() {
  local url=$1; local timeout=${2:-30}; local msg=${3:-"Waiting"}
  local start=$(date +%s)
  while true; do
    if http_ok "$url"; then return 0; fi
    local now=$(date +%s)
    if (( now - start >= timeout )); then return 1; fi
    printf "\r%s... (%s)" "$msg" "$url"
    sleep 1
  done
}

store_has_instance() {
  # Grep raw JSON to avoid jq dependency
  local url=$1; local inst=$2
  local body
  body=$(curl -sf "$url" 2>/dev/null || true)
  [[ "$body" =~ "\"instances\"" ]] || return 1
  echo "$body" | grep -q "\"$inst\""
}

say "Checking OpB1 health at $OPB1_HTTP/healthz"
if ! wait_for_http "$OPB1_HTTP/healthz" 60 "Waiting for OpB1 to be healthy"; then
  echo
  say "ERROR: OpB1 is not healthy after 60s. Please ensure it's running before starting this demo."
  exit 1
fi
echo "\nOpB1 OK"

# Ensure store-touch topic exists (compacted)
if command -v kafka-topics >/dev/null 2>&1; then
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic p1.opb-store-touch --partitions 3 --replication-factor 1 --config cleanup.policy=compact >/dev/null 2>&1 || true
elif [ -x /opt/homebrew/bin/kafka-topics ]; then
  /opt/homebrew/bin/kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic p1.opb-store-touch --partitions 3 --replication-factor 1 --config cleanup.policy=compact >/dev/null 2>&1 || true
fi

say "Starting OpB2 (instance $INSTANCE2_ID) to join group: $GROUP_ID"
if ! http_ok "$OPB2_HTTP/healthz"; then
  mkdir -p "$STATE2_DIR" ./logs
  OPB_PEERS="http://127.0.0.1:${OPB1_HTTP##*:},http://127.0.0.1:${OPB2_HTTP##*:}" \
  $BIN_OPB \
    --state-backend pebble --state-dir "$STATE2_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
    --input-source kafka \
    --topic-enriched p1.orders.enriched --output-topic p1.orders.output \
    --changelog-sink both --manifest-sink both \
    --topic-changelog p1.opb-changelog --topic-snapshots p1.opb-snapshots --topic-store-touch p1.opb-store-touch \
    --window-size 60 --tx-batch-size 1000 --tx-linger-ms 100 \
    --http :${OPB2_HTTP##*:} --instance-id "$INSTANCE2_ID" \
    > ./logs/opb2.out 2>&1 &
fi

if wait_for_http "$OPB2_HTTP/healthz" 30 "Waiting for OpB2 healthy"; then
  echo "\nOpB2 OK"
else
  echo "\nOpB2 NOT READY ($OPB2_HTTP/healthz)"
fi

# Give Kafka a brief moment to rebalance partitions to B2
sleep 2

say "Pumping data to spread work to both instances (store prefix: $STORE)"
STORES="$STORE" N="$PUMP_N" BATCH="$PUMP_BATCH" RANDOM_DELAY_MS_MIN=0 RANDOM_DELAY_MS_MAX=60 \
  bash scripts/pump_random.sh || true

say "Open cluster page to verify instances and assignment"
echo "$OPB1_HTTP/viz/cluster"

STORE_URL="$OPB1_HTTP/viz/zone-data?id=$STORE"
say "Open store page and verify instances include B1 and $INSTANCE2_ID (cluster-wide)"
echo "$STORE_URL"

# Auto wait until B2 shows up (up to 20s) to make the demo deterministic in non-interactive mode
if [[ "$INTERACTIVE" == "0" || "$AUTO_Y" == "1" ]]; then
  for i in {1..20}; do
    if store_has_instance "$STORE_URL" "$INSTANCE2_ID"; then
      break
    fi
    sleep 1
  done
fi

ask_continue "Did you see instances include both B1 and $INSTANCE2_ID?" || { echo Aborted; exit 1; }

say "(Optional) Demonstrate failover: kill B2 then restart"
ask_continue "Kill B2 now to see instances drop to [B1]?" && {
  # BSD pkill treats patterns starting with '-' as options; use pgrep+kill instead
  PIDS=$(pgrep -f "opb .*--instance-id $INSTANCE2_ID" || true)
  if [[ -n "${PIDS:-}" ]]; then
    echo "Killing PIDs: $PIDS"
    kill $PIDS || true
  else
    echo "No B2 PIDs found"
  fi
  sleep 2
  echo "Refresh $STORE_URL (instances should lose $INSTANCE2_ID)"
  ask_continue "Restart B2?" && {
    $BIN_OPB \
      --state-backend pebble --state-dir "$STATE2_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
      --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
      --input-source kafka \
      --topic-enriched p1.orders.enriched --output-topic p1.orders.output \
      --changelog-sink both --manifest-sink both \
      --topic-changelog p1.opb-changelog --topic-snapshots p1.opb-snapshots \
      --window-size 60 --tx-batch-size 1000 --tx-linger-ms 100 \
      --http :${OPB2_HTTP##*:} --instance-id "$INSTANCE2_ID" \
      > ./logs/opb2.out 2>&1 &
    wait_for_http "$OPB2_HTTP/healthz" 30 "Waiting for OpB2 healthy" || true
    echo "Refresh $STORE_URL (instances should show B1 and $INSTANCE2_ID again)"
    ask_continue "Done viewing?" || true
  }
}

say "Scale-out demo completed."
