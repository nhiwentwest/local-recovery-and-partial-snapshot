#!/usr/bin/env bash
set -euo pipefail

# Local Recovery & Partial Snapshot Demo (with offset reset to avoid backlog)
# Proves that a crashed OpB instance can recover its state from a snapshot and changelog.

# --- Configurable Env Vars ---
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
OPB1_HTTP=${OPB1_HTTP:-http://127.0.0.1:8089}
BIN_OPB=${BIN_OPB:-./bin/opb}
KADMIN_BIN=${KADMIN_BIN:-./bin/kadmin}
STORE=${STORE:-RECOVERY-TEST}
PROD=${PROD:-p1}
STATE_DIR=./data/opb-recovery
SNAPSHOT_DIR=./snapshots-recovery
CHANGELOG_DIR=./changelog-recovery
SNAPSHOT_INTERVAL=15 # seconds
export WINDOW_SIZE=${WINDOW_SIZE:-3600} # seconds (must match pump and WS calc)
AUTO_Y=${AUTO_Y:-0}
INTERACTIVE=${INTERACTIVE:-1}
# Shutdown/view controls
NO_SHUTDOWN=${NO_SHUTDOWN:-0}                  # 1 = do NOT stop OpB at the end
SLEEP_BEFORE_SHUTDOWN=${SLEEP_BEFORE_SHUTDOWN:-30} # default 30s for non-interactive runs
VIEW_WAIT_SEC=${VIEW_WAIT_SEC:-0}              # extra seconds to wait after restart before proceeding
# Retries
CHECK_EXACT_RETRIES=${CHECK_EXACT_RETRIES:-30}  # seconds to wait for Exact mode to show up (reduced)

wait_snapshot_created() {
  local timeout=${1:-30}
  local i
  say "Waiting up to ${timeout}s for snapshot + manifest publish..."
  for ((i=1;i<=timeout;i++)); do
    if grep -q "snapshot and manifest published:" ./logs/recovery_b1.out 2>/dev/null; then
      say "Snapshot + manifest detected in logs."
      return 0
    fi
    sleep 1
  done
  say "WARN: snapshot publish not observed within timeout; continuing"
  return 1
}

# Stable group-id across start & restart (can override via env)
GROUP_ID=${GROUP_ID:-opb-recovery-$(date +%s)}
export GROUP_ID

# --- Helper Functions ---
say() { printf "\n\e[1;36m[RECOVERY-DEMO]\e[0m %s\n" "$*"; }
http_ok() { curl -sf "$1" >/dev/null 2>&1; }
require_kadmin() {
  if [ ! -x "$KADMIN_BIN" ]; then
    say "Building kadmin helper..."
    go build -o "$KADMIN_BIN" ./cmd/kadmin
  fi
}
kadmin() {
  "$KADMIN_BIN" -bootstrap "$BOOTSTRAP" "$@"
}

require_jq() {
  if ! command -v jq >/dev/null 2>&1; then
    say "ERROR: jq is required for parsing JSON. Please install jq (e.g., brew install jq) and retry."
    exit 1
  fi
}

post_inject() {
  local payload=$1
  local tries=${2:-8}
  local i resp
  for ((i=1;i<=tries;i++)); do
    resp=$(curl -s -X POST -H 'Content-Type: application/json' -d "$payload" "$OPB1_HTTP/api/inject-test-data" || true)
    if echo "$resp" | grep -qi 'rate limited'; then
      say "inject rate-limited, retry in 2s ($i/$tries)"; sleep 2; continue
    fi
    echo "$resp"; return 0
  done
  echo "$resp"; return 0
}

get_lastseq() {
  local url=$1; local data ls
  data=$(curl -s "$url" || true)
  ls=$(echo "$data" | grep -E '"lastSeq"' | head -n1 | sed -E 's/.*"lastSeq"\: *([0-9-]+).*/\1/' )
  if [[ -z "$ls" ]]; then echo 0; else echo "$ls"; fi
}

get_exact_sumqty() {
  local url=$1; local data sq
  data=$(curl -s "$url" || true)
  # Extract sumQty using sed-only to avoid grep class issues
  sq=$(printf '%s' "$data" | sed -n 's/.*"sumQty"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
  if [[ -z "$sq" ]]; then echo 0; else echo "$sq"; fi
}

get_heatmap_total() {
  local url=$1; local store=$2; local data value
  # Cache-buster to avoid any intermediary caching
  local ts=$(date +%s%3N 2>/dev/null || date +%s)
  data=$(curl -s "${url}&_ts=${ts}" || true)
  # Parse JSON to find cell with matching storeId
  if command -v jq >/dev/null 2>&1; then
    value=$(echo "$data" | jq -r ".cells[] | select(.storeId == \"$store\") | .value" 2>/dev/null | head -n1)
    if [[ -n "$value" && "$value" != "null" ]]; then echo "$value"; else echo "0"; fi
  else
    # Fallback: grep/sed parsing
    value=$(echo "$data" | grep -o "\"storeId\":\"$store\"[^}]*\"value\":[0-9]*" | sed -E 's/.*"value":([0-9]*).*/\1/' | head -n1)
    if [[ -n "$value" ]]; then echo "$value"; else echo "0"; fi
  fi
}

get_debug_total() {
  local store=$1; local data tot
  # Cache-buster to avoid any caching
  local ts=$(date +%s%3N 2>/dev/null || date +%s)
  data=$(curl -s "$OPB1_HTTP/api/debug-store-keys?storeId=$store&_ts=${ts}" || true)
  if command -v jq >/dev/null 2>&1; then
    tot=$(echo "$data" | jq -r '.totalSumQty' 2>/dev/null)
  else
    # sed-only to avoid grep bracket issues on BSD/macOS
    tot=$(printf '%s' "$data" | sed -n 's/.*"totalSumQty"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
  fi
  if [[ -z "$tot" || "$tot" == "null" ]]; then echo 0; else echo "$tot"; fi
}

get_lag_total() {
  local data lag
  data=$(curl -s "$OPB1_HTTP/status" || true)
  if command -v jq >/dev/null 2>&1; then
    lag=$(echo "$data" | jq -r '.lagTotal' 2>/dev/null)
  else
    # sed-only, extract number (may be float), then floor
    lag=$(printf '%s' "$data" | sed -n 's/.*"lagTotal"[[:space:]]*:[[:space:]]*\([0-9.][0-9.]*\).*/\1/p' | head -n1)
  fi
  # floor to integer for comparison
  if [[ -z "$lag" || "$lag" == "null" ]]; then echo 0; else printf '%.0f' "$lag"; fi
}

wait_heatmap_value() {
  local url=$1; local store=$2; local expected=$3; local timeout=${4:-30}; local i
  say "Waiting for heatmap to show $expected for store $store (up to ${timeout}s)..."
  for ((i=1;i<=timeout;i++)); do
    local current; current=$(get_heatmap_total "$url" "$store")
    printf "\r  [%d/%d] heatmap=$current (expected=$expected)" "$i" "$timeout"
    if [[ "$current" =~ ^[0-9]+$ ]] && (( current >= expected )); then
      printf "\n"
      say "✓ Heatmap updated: $current >= $expected"
      return 0
    fi
    sleep 1
  done
  printf "\n"
  local final; final=$(get_heatmap_total "$url" "$store")
  say "WARN: Heatmap timeout (final=$final, expected=$expected)"
  return 1
}

wait_for_exact() {
  local url=$1; local min_ls=${2:-0}; local attempts=${3:-$CHECK_EXACT_RETRIES}; local i
  for ((i=1;i<=attempts;i++)); do
    local ls; ls=$(get_lastseq "$url")
    if [[ "$ls" =~ ^[0-9]+$ ]] && (( ls > 0 )) && (( ls >= min_ls )); then
      return 0
    fi
    sleep 1; printf "."
  done
  return 1
}

reset_group_offsets() {
  local grp=$1
  say "Deleting consumer group ${grp} to reset offsets"
  kadmin -cmd delete-group -group "$grp" || true
}

delete_topic_if_exists() {
  local topic=$1
  say "Deleting topic if exists: ${topic}"
  kadmin -cmd delete -topic "$topic" || true
}

if [[ "$AUTO_Y" != "1" && "$INTERACTIVE" != "0" && ! -t 0 && -e /dev/tty ]]; then
  exec </dev/tty
fi

ask_continue() {
  local msg=${1:-"Press y to continue, n to abort"}
  if [[ "$AUTO_Y" == "1" ]]; then return 0; fi
  if [[ "$INTERACTIVE" == "0" ]]; then return 0; fi
  local ans=""; while true; do read -r -p "${msg} [y/n]: " ans || true; case "$ans" in y|Y) return 0;; n|N) return 1;; *) echo "Please answer y or n.";; esac; done
}

wait_ready() {
  local url=$1; local n=${2:-60}
  say "Waiting for $url to be healthy (up to ${n}s)..."
  for((i=0;i<n;i++)); do if http_ok "$url"; then echo " OK"; return 0; fi; sleep 1; printf "."; done; echo " ERROR"; return 1
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

# --- Cleanup leftover ---
say "Stopping all OpB instances to free port 8089..."
pkill -f opb >/dev/null 2>&1 || true
sleep 2
ensure_port_free 8089

# --- Main Demo Logic ---

require_kadmin
require_jq

say "Phase 1: Setup & Create Snapshot"

say "Clean up old state, snapshots, and logs"
rm -rf "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR"
mkdir -p "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR" ./logs

say "Reset Kafka topics for clean demo"
delete_topic_if_exists "p1.opb-snapshots"
delete_topic_if_exists "p1.opb-changelog"

# Reset offsets to latest to avoid huge backlog
ENRICHED_TOPIC=${ENRICHED_TOPIC:-p1.orders.enriched}
# Ensure demo starts clean: delete enriched topic then recreate
say "Deleting enriched topic to ensure clean demo (no old windows)"
delete_topic_if_exists "$ENRICHED_TOPIC"
# Create enriched topic
ensure_enriched_topic() {
  local topic=$1
  say "Ensuring enriched topic exists: ${topic}"
  kadmin -cmd create -topic "$topic" -partitions 4 -rf 1 >/dev/null 2>&1 || true
}
ensure_enriched_topic "$ENRICHED_TOPIC"
reset_group_offsets "$GROUP_ID"

# Ensure compacted topics exist for manifest and changelog (Kafka-mode)
ensure_compacted_topic() {
  local topic=$1
  say "Ensuring compacted topic exists: ${topic}"
  kadmin -cmd create -topic "$topic" -partitions 1 -rf 1 -config "cleanup.policy=compact" >/dev/null 2>&1 || true
}
ensure_compacted_topic "p1.opb-snapshots"
# Changelog should NOT be compacted for delta replay; keep full history
ensure_delete_topic() {
  local topic=$1
  say "Ensuring delete-policy topic exists: ${topic}"
  kadmin -cmd create -topic "$topic" -partitions 1 -rf 1 -config "cleanup.policy=delete" >/dev/null 2>&1 || true
}
ensure_delete_topic "p1.opb-changelog"

say "Start OpB (B1) with PebbleDB and snapshot interval=${SNAPSHOT_INTERVAL}s"
EXTRA_OPB_FLAGS="--topic-snapshots p1.opb-snapshots --topic-changelog p1.opb-changelog --manifest-sink both --manifest-source kafka --changelog-sink both --changelog-source kafka"
"$BIN_OPB" \
  --state-backend pebble --state-dir "$STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
  --changelog-dir "$CHANGELOG_DIR" \
  --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE" \
  $EXTRA_OPB_FLAGS \
  --http :8089 --instance-id B1 > ./logs/recovery_b1.out 2>&1 &
OPB_PID=$!

if ! wait_ready "$OPB1_HTTP/healthz" 180; then echo "ERROR: B1 failed to start"; tail -n 200 ./logs/recovery_b1.out || true; exit 1; fi

NOW=$(date +%s)
WS=$(( (NOW/ WINDOW_SIZE) * WINDOW_SIZE ))
USE_FUTURE_WINDOW=${USE_FUTURE_WINDOW:-0}
if [[ "$USE_FUTURE_WINDOW" == "1" ]]; then WS=$(( WS + WINDOW_SIZE )); fi
# Use store-total mode (no productId/ws) to match heatmap total aggregation
EXACT_URL="$OPB1_HTTP/api/zone-details?id=$STORE"
EXACT_URL_WS="$OPB1_HTTP/api/zone-details?id=$STORE&productId=$PROD&ws=$WS"
HEATMAP_URL="$OPB1_HTTP/viz/heatmap?metric=total"

say "Wait for initial snapshot (empty baseline) before pumping delta"
wait_snapshot_created 30
# Baseline without initial pump
base_ls=0
base_sq=$(get_exact_sumqty "$EXACT_URL")
say "Checkpoint 0: Baseline before delta"
echo "Exact URL (store-total): $EXACT_URL"
echo "Exact URL (window): $EXACT_URL_WS"
echo "Exact sumQty (total)=$base_sq lastSeq=$base_ls"
# Debug: list all keys for this store
say "Debug: All keys for store $STORE:"
curl -s "$OPB1_HTTP/api/debug-store-keys?storeId=$STORE" | jq '.' 2>/dev/null || curl -s "$OPB1_HTTP/api/debug-store-keys?storeId=$STORE"

say "Phase 2: Create Delta Data (in changelog only)"
ask_continue "Pump delta 500 events (after snapshot)?" || { say "User aborted delta pump"; exit 1; }
say "Pump 500 more events (delta after snapshot)"
RESP2=$(post_inject "{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":500,\"start\":1000}")
if command -v jq >/dev/null 2>&1; then echo "$RESP2" | jq .; else echo "$RESP2"; fi

say "Waiting delta to fully apply (+500): exact.lastSeq, state.totalSumQty, and lagTotal=0"
min_ls=$(( base_ls + 500 ))
expected_total=$(( base_sq + 500 ))
WAIT_DELTA_DEADLINE=$(( $(date +%s) + 20 ))
converged=0
while true; do
  cur_ls=$(get_lastseq "$EXACT_URL_WS")
  cur_tot=$(get_debug_total "$STORE")
  cur_lag=$(get_lag_total)
  cur_sq=$(get_exact_sumqty "$EXACT_URL")
  printf "\r.. ls=%d/%d total=%d/%d lag=%d" "$cur_ls" "$min_ls" "$cur_tot" "$expected_total" "$cur_lag"
  if (( cur_ls >= min_ls )) && (( cur_tot >= expected_total )) && (( cur_lag == 0 )); then
    converged=1; break
  fi
  if (( $(date +%s) > WAIT_DELTA_DEADLINE )); then
    printf "\n"; say "ERROR: delta not fully applied within deadline (ls=$cur_ls total=$cur_tot lag=$cur_lag). Aborting to avoid inconsistent checkpoint."
    # Print logs tail for diagnostics
    tail -n 120 ./logs/recovery_b1.out || true
    exit 1
  fi
  sleep 1
done
printf "\n"

say "Checkpoint 2: State BEFORE crash"
echo "Exact URL (store-total): $EXACT_URL"
echo "Exact URL (window): $EXACT_URL_WS"
echo "Exact sumQty (total)=$cur_sq lastSeq=$cur_ls (expected lastSeq>=$min_ls)"
# Debug: list all keys for this store
say "Debug: All keys for store $STORE:"
curl -s "$OPB1_HTTP/api/debug-store-keys?storeId=$STORE" | jq '.' 2>/dev/null || curl -s "$OPB1_HTTP/api/debug-store-keys?storeId=$STORE"
# Wait for heatmap to reflect the delta (+500 events)
wait_heatmap_value "$HEATMAP_URL" "$STORE" "$cur_sq" 15
heatmap_val2=$(get_heatmap_total "$HEATMAP_URL" "$STORE")
say "✓ Heatmap Checkpoint 2: $heatmap_val2 (expected ~$cur_sq)"

# Ensure a manifest is actually published to Kafka before crashing, to avoid race at restart
wait_manifest_published() {
  local timeout=${1:-45}
  local i
  say "Waiting up to ${timeout}s for manifest publish (logs)..."
  for ((i=1;i<=timeout;i++)); do
    if grep -q "snapshot and manifest published:" ./logs/recovery_b1.out 2>/dev/null; then
      say "Manifest publish detected in logs."
      return 0
    fi
    sleep 1
  done
  say "WARN: manifest not observed within timeout; continuing anyway"
}
wait_manifest_published 45

ask_continue "Ready to crash the instance?" || { echo Aborted; exit 1; }

say "Phase 3: Crash & Recovery"
say "Inducing fault (kill -9)"
kill -9 "$OPB_PID" || true
say "Waiting for process $OPB_PID to fully exit..."
for i in {1..100}; do if ! kill -0 "$OPB_PID" >/dev/null 2>&1; then break; fi; sleep 0.1; printf "."; done; echo

# Verify snapshot exists before recovery
say "Verifying snapshot exists before recovery..."
SNAPSHOT_ID=$(grep -o '"snapshotId":"[^"]*"' ./logs/recovery_b1.out 2>/dev/null | tail -n1 | sed 's/.*"snapshotId":"\([^"]*\)".*/\1/' || true)
if [[ -n "$SNAPSHOT_ID" ]]; then
  SNAPSHOT_FILE="$SNAPSHOT_DIR/$SNAPSHOT_ID/state.json"
  if [[ -f "$SNAPSHOT_FILE" ]]; then
    SNAPSHOT_SIZE=$(stat -f%z "$SNAPSHOT_FILE" 2>/dev/null || stat -c%s "$SNAPSHOT_FILE" 2>/dev/null || echo "unknown")
    say "Snapshot found: $SNAPSHOT_ID (size: $SNAPSHOT_SIZE bytes)"
  else
    say "WARN: Snapshot file not found at $SNAPSHOT_FILE"
  fi
else
  say "WARN: Could not extract snapshotId from logs"
fi

say "Restarting B1 in two stages..."

say "Stage 1: Restore-only to rebuild state and exit"
rm -f "$STATE_DIR/LOCK" 2>/dev/null || true # Clean lock before restore
"$BIN_OPB" \
  --state-backend pebble --state-dir "$STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
  $EXTRA_OPB_FLAGS \
  --window-size "$WINDOW_SIZE" \
  --http :8089 --instance-id B1 \
  --restore-on-start --restore-only >> ./logs/recovery_b1.out 2>&1

# Verify snapshot was restored
say "Verifying snapshot restoration..."
if grep -q "restore: snapshot restored" ./logs/recovery_b1.out 2>/dev/null; then
  RESTORED_SNAPSHOT=$(grep "restore: snapshot restored" ./logs/recovery_b1.out | tail -n1 | sed -E 's/.*snapshotId=([^ ]+).*/\1/' || echo "")
  if [[ -n "$RESTORED_SNAPSHOT" ]]; then
    say "✓ Snapshot restored successfully: $RESTORED_SNAPSHOT"
  fi
  # Extract number of keys loaded from snapshot (format: "restore: loaded X keys from snapshot Y")
  KEYS_LOADED=$(grep "restore: loaded.*keys from snapshot" ./logs/recovery_b1.out | tail -n1 | sed -E 's/.*loaded ([0-9]+) keys.*/\1/' || echo "")
  if [[ -n "$KEYS_LOADED" && "$KEYS_LOADED" =~ ^[0-9]+$ ]]; then
    say "✓ Loaded $KEYS_LOADED keys from snapshot"
  fi
else
  say "WARN: Snapshot restoration log not found"
fi

say "Stage 2: Start normally to begin consuming"
"$BIN_OPB" \
  --state-backend pebble --state-dir "$STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
  $EXTRA_OPB_FLAGS \
  --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE" \
  --http :8089 --instance-id B1 >> ./logs/recovery_b1.out 2>&1 &
OPB_PID2=$!

if ! wait_ready "$OPB1_HTTP/healthz" 180; then echo "ERROR: B1 failed to start after restore"; tail -n 400 ./logs/recovery_b1.out || true; exit 1; fi

say "Phase 4: Verification"
if wait_for_exact "$EXACT_URL_WS" "$cur_ls" $CHECK_EXACT_RETRIES; then
  after_ls=$(get_lastseq "$EXACT_URL_WS"); after_sq=$(get_exact_sumqty "$EXACT_URL")
else
  after_ls=$(get_lastseq "$EXACT_URL_WS"); after_sq=$(get_exact_sumqty "$EXACT_URL")
fi
say "Checkpoint 3: State AFTER recovery"
echo "Exact URL (store-total): $EXACT_URL"
echo "Exact URL (window): $EXACT_URL_WS"
echo "Exact sumQty (total, after)=$after_sq lastSeq(after)=$after_ls (expected lastSeq>=$cur_ls and sumQty==$cur_sq)"
# Debug: list all keys for this store
say "Debug: All keys for store $STORE:"
curl -s "$OPB1_HTTP/api/debug-store-keys?storeId=$STORE" | jq '.' 2>/dev/null || curl -s "$OPB1_HTTP/api/debug-store-keys?storeId=$STORE"
# Wait for heatmap to reflect recovered state (should match pre-crash value)
wait_heatmap_value "$HEATMAP_URL" "$STORE" "$after_sq" 30
heatmap_val3=$(get_heatmap_total "$HEATMAP_URL" "$STORE")
say "✓ Heatmap Checkpoint 3: $heatmap_val3 (expected ~$after_sq, should match pre-crash ~$cur_sq)"

say "Check /status for TTR and recovery stats:"
echo "$OPB1_HTTP/status"
curl -s "$OPB1_HTTP/status" | sed -n '1,200p' || true

say "Recovery demo completed."

if [[ "$NO_SHUTDOWN" == "1" ]]; then
  say "NO_SHUTDOWN=1 set: leaving OpB running (PID1=${OPB_PID}, PID2=${OPB_PID2})."; exit 0
fi

if [[ "$INTERACTIVE" == "1" ]]; then
  read -r -p "Press Enter to stop OpB and finish demo..." _ || true
else
  if [[ "$SLEEP_BEFORE_SHUTDOWN" =~ ^[0-9]+$ ]] && [[ "$SLEEP_BEFORE_SHUTDOWN" -gt 0 ]]; then
    say "Sleeping ${SLEEP_BEFORE_SHUTDOWN}s before shutdown (SLEEP_BEFORE_SHUTDOWN)"; sleep "$SLEEP_BEFORE_SHUTDOWN"
  fi
fi

say "Stopping OpB (PID=${OPB_PID2})..."; kill "${OPB_PID2}" 2>/dev/null || true
for i in {1..50}; do if ! kill -0 "${OPB_PID2}" >/dev/null 2>&1; then break; fi; sleep 0.1; done
say "Stopping OpB (PID=${OPB_PID})..."; kill "${OPB_PID}" 2>/dev/null || true
for i in {1..50}; do if ! kill -0 "${OPB_PID}" >/dev/null 2>&1; then break; fi; sleep 0.1; done
say "Done."
