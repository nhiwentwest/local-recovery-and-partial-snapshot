#!/usr/bin/env bash
set -eo pipefail

# Two-stage scale-out demo
# Stage 1: 4 partitions, instances B1 (:8089), B2 (:8090) -> pump modest load, show /viz/cluster and /api/cluster
# Stage 2: scale partitions to 8, start B3 (:8091), B4 (:8092), pump big load, show /viz/cluster and /api/cluster
#
# Env:
# - BOOTSTRAP (default 127.0.0.1:9092)
# - OPB1_HTTP (default http://127.0.0.1:8089)
# - OPB2_HTTP (default http://127.0.0.1:8090)
# - OPB3_HTTP (default http://127.0.0.1:8091)
# - OPB4_HTTP (default http://127.0.0.1:8092)
# - STORE (default EOS-TEST-D-)
# - INTERACTIVE=0 to auto-continue with countdown
# - AUTO_Y=1 to skip prompts

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
OPB1_HTTP=${OPB1_HTTP:-http://127.0.0.1:8089}
OPB2_HTTP=${OPB2_HTTP:-http://127.0.0.1:8090}
OPB3_HTTP=${OPB3_HTTP:-http://127.0.0.1:8091}
OPB4_HTTP=${OPB4_HTTP:-http://127.0.0.1:8092}
BIN_OPB=${BIN_OPB:-./bin/opb}
STORE=${STORE:-EOS-TEST-D-}
GROUP_ID=${GROUP_ID:-opb}
STATE_DIR=${STATE_DIR:-./data}
SNAPSHOT_DIR=${SNAPSHOT_DIR:-./snapshots}
AUTO_Y=${AUTO_Y:-0}
INTERACTIVE=${INTERACTIVE:-1}
# Defaults to avoid unbound variable when set -u is disabled, but keep explicit defaults
NEW_GROUP=${NEW_GROUP:-0}
GROUP_ID_BASE=${GROUP_ID_BASE:-opb}
# Guards to prevent re-entering outage/reset blocks
DID_OUTAGE=${DID_OUTAGE:-0}
DID_RESET=${DID_RESET:-0}

if [[ "$AUTO_Y" != "1" && "$INTERACTIVE" != "0" && ! -t 0 && -e /dev/tty ]]; then
  exec </dev/tty
fi

say() { printf "\n\e[1;36m[2STAGE]\e[0m %s\n" "$*"; }
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
ensure_topic_partitions() {
  local topic=$1; local parts=$2
  local cur=$(partition_count "$topic")
  if [[ -z "$cur" || "$cur" == "0" ]]; then
    say "Creating topic $topic (partitions=$parts)"
    ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd create -topic "$topic" -partitions "$parts" -rf 1 -wait 30 >/dev/null || true
    wait_topic_exists "$topic" 30 || { say "ERROR: $topic not created in time"; exit 1; }
    return 0
  fi
  if (( parts > cur )); then
    ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd increase -topic "$topic" -partitions "$parts" -wait 30 >/dev/null || true
    wait_partitions "$topic" "$parts" 30 || { say "ERROR: partitions not increased"; exit 1; }
  fi
}

enforce_topic_exact_parts() {
  local topic=$1; local want=$2
  local cur=$(partition_count "$topic")
  # create if missing
  if [[ -z "$cur" || "$cur" == "0" ]]; then
    say "Creating topic $topic (partitions=$want)"
    ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd create -topic "$topic" -partitions "$want" -rf 1 -wait 30 >/dev/null || true
    wait_topic_exists "$topic" 30 || { say "ERROR: $topic not created in time"; exit 1; }
    wait_partitions "$topic" "$want" 30 || { say "ERROR: partitions not converged to $want"; exit 1; }
    return 0
  fi
  # already exact
  if [[ "$cur" == "$want" ]]; then return 0; fi
  # increase if lower
  if (( cur < want )); then
    say "Increasing $topic partitions $cur -> $want"
    ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd increase -topic "$topic" -partitions "$want" -wait 30 >/dev/null || true
    wait_partitions "$topic" "$want" 30 || { say "ERROR: partitions not increased to $want"; exit 1; }
    return 0
  fi
  # downscale required: delete + recreate
  say "Downscaling $topic $cur -> $want (delete & recreate)"
  # quiesce producers to avoid stuck deletion
  pkill -f "bin/opa" || true
  pkill -f "scripts/pump_random.sh" || true
  for i in 1 2 3; do
    ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd delete -topic "$topic" -wait 20 || true
    if wait_topic_deleted "$topic" 20; then break; fi
    [[ "$i" -eq 3 ]] && { say "ERROR: $topic not deleted in time"; exit 1; }
    sleep 2
  done
  ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd create -topic "$topic" -partitions "$want" -rf 1 -wait 30 || true
  wait_topic_exists "$topic" 30 || { say "ERROR: $topic not recreated in time"; exit 1; }
  wait_partitions "$topic" "$want" 60 || { say "ERROR: partitions did not converge to $want"; exit 1; }
}

require_kadmin() {
  if [ ! -x ./bin/kadmin ]; then
    say "ERROR: ./bin/kadmin not found. Please build it: go build -o bin/kadmin ./cmd/kadmin"
    exit 1
  fi
}
create_compacted_topic_if_needed() {
  local topic=$1
  # Create without special configs using kadmin; compaction optional for demo
  ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd create -topic "$topic" -partitions 3 -rf 1 -wait 10 >/dev/null || true
}
wait_ready() {
  local url=$1; local timeout=${2:-30}
  for ((i=0;i<timeout;i++)); do http_ok "$url" && return 0; sleep 1; done; return 1
}
ensure_port_free() {
  local port=$1
  # macOS: lsof to find process using port
  if command -v lsof >/dev/null 2>&1; then
    local pid
    pid=$(lsof -ti tcp:"$port" 2>/dev/null || true)
    if [[ -n "$pid" ]]; then
      say "Port :$port is busy (pid=$pid). Killing it."
      kill -9 $pid || true
      sleep 1
    fi
  fi
}
# Estimate cluster member count via /api/cluster (count healthy instances)
cluster_member_count() {
  local url="$OPB1_HTTP/api/cluster"
  local body
  body=$(curl -sf "$url" 2>/dev/null || true)
  if [[ -z "$body" ]]; then echo 0; return; fi
  echo "$body" | grep -o '"status":"healthy"' | wc -l | awk '{print $1}'
}
wait_cluster_members() {
  local expected=$1 timeout=${2:-60}
  for ((i=0;i<timeout;i++)); do
    local c=$(cluster_member_count)
    if [[ "$c" -ge "$expected" ]]; then return 0; fi
    sleep 1
  done
  return 1
}
partition_count() {
  local topic=$1
  local line
  line=$(./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd describe -topic "$topic" 2>/dev/null || true)
  # expected: topic=<name> exists=true partitions=N
  if echo "$line" | grep -q "exists=true"; then
    echo "$line" | awk '{for(i=1;i<=NF;i++){if($i ~ /^partitions=/){split($i,a,"="); print a[2]; exit}}}'
    return 0
  fi
  echo 0
}
topic_exists() {
  local topic=$1
  local line
  line=$(./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd describe -topic "$topic" 2>/dev/null || true)
  if echo "$line" | grep -q "exists=true"; then return 0; fi
  return 1
}
wait_partitions() {
  local topic=$1 want=$2 timeout=${3:-60}
  for ((i=0;i<timeout;i++)); do
    local pc=$(partition_count "$topic")
    if [[ "$pc" == "$want" ]]; then return 0; fi
    sleep 1
  done
  return 1
}
wait_topic_deleted() {
  local topic=$1 timeout=${2:-60}
  for ((i=0;i<timeout;i++)); do
    if ! topic_exists "$topic"; then return 0; fi
    sleep 1
  done
  return 1
}
wait_topic_exists() {
  local topic=$1 timeout=${2:-60}
  for ((i=0;i<timeout;i++)); do
    if topic_exists "$topic"; then return 0; fi
    sleep 1
  done
  return 1
}
start_instance() {
  local http_url=$1; local instance_id=$2; local port=${http_url##*:}; local extra_flags=${3:-}

  # Ensure no stale process/port before starting
  stop_instance "$http_url" "$instance_id" || true
  ensure_port_free "$port"

  # Default flags for scale-out demo (in-memory state)
  local base_flags="--state-backend memory --window-size 60 --tx-batch-size 1000 --tx-linger-ms 100"
  if [[ -n "$extra_flags" ]]; then
    base_flags+=" $extra_flags"
  fi

  OPB_PEERS="http://127.0.0.1:${OPB1_HTTP##*:},http://127.0.0.1:${OPB2_HTTP##*:},http://127.0.0.1:${OPB3_HTTP##*:},http://127.0.0.1:${OPB4_HTTP##*:}" \
  "$BIN_OPB" \
    $base_flags \
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
    --input-source kafka \
    --topic-enriched p1.orders.enriched --output-topic p1.orders.output \
    --changelog-sink both --manifest-sink both \
    --topic-changelog p1.opb-changelog --topic-snapshots p1.opb-snapshots --topic-store-touch p1.opb-store-touch \
    --http :${port} --instance-id "$instance_id" \
    > ./logs/${instance_id}.out 2>&1 &
}
stop_instance() {
  local http_url=$1; local instance_id=$2; local port=${http_url##*:}
  # Try kill by instance-id first, then by port
  pkill -f "opb .*--instance-id ${instance_id}" || true
  sleep 0.5
  pkill -f ":${port}" || true
}

require_kadmin

say "Prepare topics (store-touch compacted) and set partitions=4 for p1.orders.enriched"
create_compacted_topic_if_needed p1.opb-store-touch

# FORCE p1.orders.enriched to 4 partitions BEFORE starting anything
say "FORCE reset p1.orders.enriched to 4 partitions (quiesce producers)"
pkill -f "bin/opa" || true
pkill -f "scripts/pump_random.sh" || true
# Delete with guarded retries (avoid stuck deletion)
for i in 1 2 3; do
  ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd delete -topic p1.orders.enriched -wait 20 || true
  if wait_topic_deleted p1.orders.enriched 20; then
    say "Topic deleted"
    break
  fi
  [[ "$i" -eq 3 ]] && { say "ERROR: p1.orders.enriched not deleted in time"; exit 1; }
  sleep 2
done
# Create with 4 partitions and verify
./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd create -topic p1.orders.enriched -partitions 4 -rf 1 -wait 30 || true
wait_topic_exists p1.orders.enriched 30 || { say "ERROR: p1.orders.enriched not recreated in time"; exit 1; }
wait_partitions p1.orders.enriched 4 60 || { say "ERROR: partitions did not converge to 4"; exit 1; }
# Print actual state for sanity
./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd describe -topic p1.orders.enriched | sed -n '1,3p'

say "Stage 1: restart B1 and B2 to ensure full OPB_PEERS is applied"
say "BOOTSTRAP=$BOOTSTRAP (kadmin/opb)"
# Always restart B1/B2 so that OPB_PEERS includes all four instances from the beginning
pkill -f ":${OPB1_HTTP##*:}" || true
pkill -f ":${OPB2_HTTP##*:}" || true
sleep 1
GROUP_ID=opb
export GROUP_ID
say "Starting B1 at ${OPB1_HTTP} (group=$GROUP_ID)"
ensure_port_free "${OPB1_HTTP##*:}"
  start_instance "$OPB1_HTTP" B1
wait_ready "$OPB1_HTTP/healthz" 30 || { say "ERROR: B1 not ready in time"; exit 1; }
say "Starting B2 at ${OPB2_HTTP}"
ensure_port_free "${OPB2_HTTP##*:}"
start_instance "$OPB2_HTTP" B2
wait_ready "$OPB2_HTTP/healthz" 30 || true
# Auto-restart B2 once if assigned 0 partitions (static membership stale)
assigned_count() {
  local name=$1
  local body
  body=$(curl -sf "$OPB1_HTTP/api/cluster" 2>/dev/null || true)
  if [[ -z "$body" ]]; then echo 0; return; fi
  echo "$body" | awk -v inst="\"instance\":\""$name"\"" '
    $0 ~ inst {found=1}
    found && /"partitions"/ {
      gsub(/.*\[/, ""); gsub(/\].*/, ""); line=$0;
      if (line ~ /^\s*$/) { print 0; exit }
      n=split(line,a,","); print n; exit
    }
  '
}
# Wait up to 30s for B2 to get partitions; if still 0, restart B2 once
b2_parts=0
for i in $(seq 1 30); do
  b2_parts=$(assigned_count B2)
  if [[ "$b2_parts" -gt 0 ]]; then break; fi
  sleep 1
done
if [[ "$b2_parts" -eq 0 ]]; then
  say "B2 assigned 0 after 30s — auto-restart once"
  stop_instance "$OPB2_HTTP" B2
  start_instance "$OPB2_HTTP" B2
  wait_ready "$OPB2_HTTP/healthz" 60 || { say "ERROR: B2 not healthy after auto-restart"; exit 1; }
fi
# Auto-restart B2 once if assigned 0 partitions after 30s (static membership stale)
assigned_count() {
  local name=$1
  local body
  body=$(curl -sf "$OPB1_HTTP/api/cluster" 2>/dev/null || true)
  if [[ -z "$body" ]]; then echo 0; return; fi
  echo "$body" | awk -v inst="\"instance\":\""$name"\"" '
    $0 ~ inst {found=1}
    found && /"partitions"/ {
      gsub(/.*\[/, ""); gsub(/\].*/, ""); line=$0;
      if (line ~ /^\s*$/) { print 0; exit }
      n=split(line,a,","); print n; exit
    }
  '
}
# Wait up to 30s for B2 to get partitions; if still 0, restart B2 once
b2_parts=0
for i in $(seq 1 30); do
  b2_parts=$(assigned_count B2)
  if [[ "$b2_parts" -gt 0 ]]; then break; fi
  sleep 1
done
if [[ "$b2_parts" -eq 0 ]]; then
  say "B2 assigned 0 after 30s — auto-restart once"
  stop_instance "$OPB2_HTTP" B2
  start_instance "$OPB2_HTTP" B2
  wait_ready "$OPB2_HTTP/healthz" 60 || { say "ERROR: B2 not healthy after auto-restart"; exit 1; }
fi

ask_continue "Stage 1 ready. Pump modest load to spread work?" || { say "User skipped Stage 1 pump"; goto_stage2=1; }
if [[ "${goto_stage2:-0}" != "1" ]]; then
say "Pump modest load to spread work (N=3000)"
STORES="$STORE" N=3000 BATCH=300 RANDOM_DELAY_MS_MIN=0 RANDOM_DELAY_MS_MAX=30 bash scripts/pump_random.sh || true
fi

say "Open cluster overview + store page"
echo "$OPB1_HTTP/viz/cluster"
echo "$OPB1_HTTP/viz/zone-data?id=$STORE"
say "Cluster JSON snapshot:"
curl -s "$OPB1_HTTP/api/cluster" | sed -n '1,200p' || true
ask_continue "Proceed to Stage 2: scale to 8 partitions and add B3/B4?" || { echo Aborted; exit 0; }

say "Stage 2: scale partitions to 8"
ensure_topic_partitions p1.orders.enriched 8
sleep 2

say "Start B3 and B4"
if ! http_ok "$OPB3_HTTP/healthz"; then start_instance "$OPB3_HTTP" B3; fi
if ! http_ok "$OPB4_HTTP/healthz"; then start_instance "$OPB4_HTTP" B4; fi
wait_ready "$OPB3_HTTP/healthz" 60 || { say "ERROR: B3 failed to become healthy"; exit 1; }
wait_ready "$OPB4_HTTP/healthz" 60 || { say "ERROR: B4 failed to become healthy"; exit 1; }

# Ensure B1/B2 also have full peer list; optionally restart them to refresh peers
RESTART_B1B2_ON_STAGE2=${RESTART_B1B2_ON_STAGE2:-0}
if [[ "$RESTART_B1B2_ON_STAGE2" == "1" ]]; then
  say "Restarting B1 and B2 with full peers to refresh /api/cluster view"
  pkill -f ":${OPB1_HTTP##*:}" || true; sleep 1
  pkill -f ":${OPB2_HTTP##*:}" || true; sleep 1
  start_instance "$OPB1_HTTP" B1
  wait_ready "$OPB1_HTTP/healthz" 60 || { say "ERROR: B1 failed to become healthy after restart"; exit 1; }
  start_instance "$OPB2_HTTP" B2
  wait_ready "$OPB2_HTTP/healthz" 60 || { say "ERROR: B2 failed to become healthy after restart"; exit 1; }
fi

# Diagnose consumer group status before pumping big load
if [[ "${DIAG:-0}" == "1" ]]; then
  say "Consumer group status before load (group=$GROUP_ID)"
  if command -v kafka-consumer-groups >/dev/null 2>&1; then
    kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$GROUP_ID" | cat || true
  elif [ -x /opt/homebrew/bin/kafka-consumer-groups ]; then
    /opt/homebrew/bin/kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$GROUP_ID" | cat || true
  fi
fi

say "Pump big load to demonstrate scale (N=20000, PARALLEL=8)"
STORES="A-,B-,C-,D-,E-,F-,G-,H-,I-,J-,K-,L-,M-,N-,O-,P-,Q-,R-,S-,T-" N=20000 PARALLEL=8 BATCH=1000 RANDOM_DELAY_MS_MIN=0 RANDOM_DELAY_MS_MAX=15 bash scripts/pump_random.sh || true

say "Cluster overview after scale-up"
echo "$OPB1_HTTP/viz/cluster"
if [[ "${DIAG:-0}" == "1" ]]; then
curl -s "$OPB1_HTTP/api/cluster" | sed -n '1,200p' || true
fi

# Simulate B3/B4 temporary outage to demonstrate recovery/rebalance
if ask_continue "Simulate B3/B4 outage now (rebalance to B1/B2)?"; then
  say "Simulate B3/B4 outage -> rebalance to B1/B2"
  stop_instance "$OPB3_HTTP" B3
  stop_instance "$OPB4_HTTP" B4
  wait_cluster_members 2 60 || { say "ERROR: group did not shrink to 2 members"; exit 1; }
  sleep 2
  if [[ "${DIAG:-0}" == "1" ]]; then curl -s "$OPB1_HTTP/api/cluster" | sed -n '1,200p' || true; fi
else
  say "Skip outage simulation"
fi

# Bring B3/B4 back to demonstrate redistribution
if ask_continue "Bring B3/B4 back to redistribute across 4 members?"; then
  say "Restart B3/B4 -> redistribute across 4 members"
  start_instance "$OPB3_HTTP" B3
  start_instance "$OPB4_HTTP" B4
  wait_ready "$OPB3_HTTP/healthz" 60 || { say "ERROR: B3 failed to become healthy (after restart)"; exit 1; }
  wait_ready "$OPB4_HTTP/healthz" 60 || { say "ERROR: B4 failed to become healthy (after restart)"; exit 1; }
  wait_cluster_members "$GROUP_ID" 4 60 || { say "ERROR: group did not grow back to 4 members"; exit 1; }
  sleep 2
  if [[ "${DIAG:-0}" == "1" ]]; then curl -s "$OPB1_HTTP/api/cluster" | sed -n '1,200p' || true; fi
else
  say "Skip restart B3/B4"
fi

# Short logs tail for B3/B4 to aid troubleshooting
if [[ "${DIAG:-0}" == "1" ]]; then
  say "Recent logs for B3 and B4 (last 80 lines)"
  [[ -f ./logs/B3.out ]] && tail -n 80 ./logs/B3.out || true
  [[ -f ./logs/B4.out ]] && tail -n 80 ./logs/B4.out || true
fi

# Auto-reset partitions back after a delay
RESET_AFTER_SEC=${RESET_AFTER_SEC:-60}
RESET_PARTS=${RESET_PARTS:-4}

# Optional: skip auto-reset entirely if NO_RESET=1
if [[ "${NO_RESET:-0}" == "1" ]]; then
  say "NO_RESET=1 set: skipping auto-reset. Keep cluster at current partitions."
  echo "$OPB1_HTTP/viz/cluster"
  curl -s "$OPB1_HTTP/api/cluster" | sed -n '1,200p' || true
  exit 0
fi

# Ask before auto-reset when interactive (or just countdown if INTERACTIVE=0), unless AUTO_Y=1
if ask_continue "Proceed to auto-reset to partitions=$RESET_PARTS now?"; then
  CONFIRM_RESET=1
else
  say "User chose to skip auto-reset. Exiting."
  exit 0
fi

say "Two-stage scale-out demo completed. Auto-reset to partitions=$RESET_PARTS after $RESET_AFTER_SEC seconds"
# If user confirmed, reset immediately without waiting
if [[ "${CONFIRM_RESET:-0}" == "1" ]]; then
  say "User confirmed reset: applying now"
else
for ((i=RESET_AFTER_SEC;i>0;i--)); do printf "\rReset in %ds..." "$i"; sleep 1; done; echo
fi

# Ensure only B1/B2 remain before resetting partitions
say "Stopping B3/B4 before reset to ensure final cluster=2"
stop_instance "$OPB3_HTTP" B3
stop_instance "$OPB4_HTTP" B4
wait_cluster_members 2 60 || { say "ERROR: group did not shrink to 2 members before reset"; exit 1; }

# Decide reset strategy based on current partition count
cur_pc=$(partition_count p1.orders.enriched)
say "Current partitions: $cur_pc, target: $RESET_PARTS"
if [[ "$cur_pc" == "$RESET_PARTS" ]]; then
  say "Partitions already at target; skipping alter"
elif (( cur_pc < RESET_PARTS )); then
  say "Increasing partitions via --alter"
if command -v kafka-topics >/dev/null 2>&1; then
  kafka-topics --bootstrap-server "$BOOTSTRAP" --alter --topic p1.orders.enriched --partitions "$RESET_PARTS" || true
elif [ -x /opt/homebrew/bin/kafka-topics ]; then
  /opt/homebrew/bin/kafka-topics --bootstrap-server "$BOOTSTRAP" --alter --topic p1.orders.enriched --partitions "$RESET_PARTS" || true
fi
else
  # cur_pc > RESET_PARTS: must delete & recreate
  say "Decreasing partitions requires delete & recreate; performing hard reset"
  # Optionally quiesce producers to prevent stuck deletion
  if [[ "${QUIESCE:-1}" == "1" ]]; then
    say "Quiescing producers (OpA/pump) before delete"
    pkill -f "bin/opa" || true
    pkill -f "scripts/pump_random.sh" || true
  fi
  # Delete with retries
  say "Deleting topic p1.orders.enriched with guarded retries"
  for i in 1 2 3; do
    ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd delete -topic p1.orders.enriched -wait 20 || true
    if wait_topic_deleted p1.orders.enriched 20; then
      say "Topic deleted"
      break
    fi
    if [[ "$i" -eq 3 ]]; then
      say "ERROR: topic p1.orders.enriched not deleted in time"
      exit 1
    fi
    say "WARN: delete not complete, retry $i/3"
    sleep 2
  done
  # Recreate and verify
  ./bin/kadmin -bootstrap "$BOOTSTRAP" -cmd create -topic p1.orders.enriched -partitions "$RESET_PARTS" -rf 1 -wait 30 || true
  wait_topic_exists p1.orders.enriched 30 || { say "ERROR: topic p1.orders.enriched not recreated in time"; exit 1; }
  wait_partitions p1.orders.enriched "$RESET_PARTS" 60 || { say "ERROR: partitions did not converge after recreate"; exit 1; }
  sleep 1
  # Keep B1/B2 running during hard reset; no restart here
  say "Keeping B1/B2 running; they will re-subscribe to the recreated topic automatically"
fi
wait_partitions p1.orders.enriched "$RESET_PARTS" 60 || { say "ERROR: partitions did not converge to $RESET_PARTS"; exit 1; }

say "Partitions reset done. Open cluster overview to verify:"
echo "$OPB1_HTTP/viz/cluster"
if [[ "${DIAG:-0}" == "1" ]]; then
curl -s "$OPB1_HTTP/api/cluster" | sed -n '1,200p' || true
fi

# Final confirmation to keep window open until user acknowledges
ask_continue "Final state should be 2 instances and $RESET_PARTS partitions. Finish demo now?" || { say "Keeping processes running for inspection"; exit 0; }
# Always end the script after final state to avoid accidental re-entry of reset logic
exit 0

# Optional hard reset: delete and recreate topic with RESET_PARTS partitions (destructive)
if [[ "${RESET_MODE:-}" == "delete_recreate" ]]; then
  say "Hard reset enabled: delete and recreate topic p1.orders.enriched with partitions=$RESET_PARTS"
  say "Stopping OpB instances (B4,B3,B2,B1) if running..."
  pkill -f "./bin/opb .*:8092" || true
  pkill -f "./bin/opb .*:8091" || true
  pkill -f "./bin/opb .*:8090" || true
  pkill -f "./bin/opb .*:8089" || true
  sleep 2
  if command -v kafka-topics >/dev/null 2>&1; then
    kafka-topics --bootstrap-server "$BOOTSTRAP" --delete --topic p1.orders.enriched || true
    kafka-topics --bootstrap-server "$BOOTSTRAP" --create --topic p1.orders.enriched --partitions "$RESET_PARTS" --replication-factor 1 || true
  elif [ -x /opt/homebrew/bin/kafka-topics ]; then
    /opt/homebrew/bin/kafka-topics --bootstrap-server "$BOOTSTRAP" --delete --topic p1.orders.enriched || true
    /opt/homebrew/bin/kafka-topics --bootstrap-server "$BOOTSTRAP" --create --topic p1.orders.enriched --partitions "$RESET_PARTS" --replication-factor 1 || true
  fi
  say "Restarting B1 and B2"
  start_instance "$OPB1_HTTP" B1
  wait_ready "$OPB1_HTTP/healthz" 60 || true
  start_instance "$OPB2_HTTP" B2
  wait_ready "$OPB2_HTTP/healthz" 60 || true
  say "Cluster overview after hard reset"
  echo "$OPB1_HTTP/viz/cluster"
  curl -s "$OPB1_HTTP/api/cluster" | sed -n '1,200p' || true
fi

