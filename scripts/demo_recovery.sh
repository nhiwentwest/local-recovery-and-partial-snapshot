#!/usr/bin/env bash
set -euo pipefail

# Local Recovery & Partial Snapshot Demo (Barrier-based Non-blocking Snapshot)
# Demonstrates recovery using partial snapshot with per-partition changelog offsets captured in manifest.

# --- Configurable Env Vars ---
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
OPB1_HTTP=${OPB1_HTTP:-http://127.0.0.1:8089}
OPB2_HTTP=${OPB2_HTTP:-http://127.0.0.1:8090}
OPB3_HTTP=${OPB3_HTTP:-http://127.0.0.1:8091}
BIN_OPB=${BIN_OPB:-./bin/opb}
BIN_OPBTOOL=${BIN_OPBTOOL:-./bin/opbtool}
KADMIN_BIN=${KADMIN_BIN:-./bin/kadmin}
STORE=${STORE:-RECOVERY-TEST}
PROD=${PROD:-p1}
STATE_DIR=./data/opb-recovery
SNAPSHOT_DIR=./snapshots-recovery
CHANGELOG_DIR=./changelog-recovery
STATE_DIR_B3=${STATE_DIR_B3:-${STATE_DIR}.b3}
CHANGELOG_DIR_B3=${CHANGELOG_DIR_B3:-${CHANGELOG_DIR}.b3}
OPB1_LOG=./logs/recovery_b1.out
OPB2_LOG=./logs/recovery_b2.out
OPB3_LOG=./logs/recovery_b3.out
OPB2_HTTP_ADDR=${OPB2_HTTP_ADDR:-:8090}
OPB3_HTTP_ADDR=${OPB3_HTTP_ADDR:-:8091}
MANIFEST_INFLIGHT_FILE=""
DELTA_STORES=("RECOVERY-A" "RECOVERY-B" "RECOVERY-C" "RECOVERY-D")
# Giảm backlog mặc định để delta gọn hơn (giảm SST và TTR); có thể tăng qua env khi cần tải lớn
DELTA_EVENTS_PER_STORE=${DELTA_EVENTS_PER_STORE:-5000}
DELTA_BASE_EVENTS=${DELTA_BASE_EVENTS:-10000}
POST_CUT_EVENTS=${POST_CUT_EVENTS:-1000}
BASELINE_EVENTS=${BASELINE_EVENTS:-0}
ENRICHED_PARTITIONS=${ENRICHED_PARTITIONS:-12}
CHANGELOG_PARTITIONS=${CHANGELOG_PARTITIONS:-4}
SNAPSHOTS_PARTITIONS=${SNAPSHOTS_PARTITIONS:-2}
EXPECTED_PARTITIONS=${EXPECTED_PARTITIONS:-$ENRICHED_PARTITIONS}
EXPECTED_PARTITIONS_PER_NODE=${EXPECTED_PARTITIONS_PER_NODE:-$(( ENRICHED_PARTITIONS / 3 ))}
if (( EXPECTED_PARTITIONS_PER_NODE < 1 )); then
  EXPECTED_PARTITIONS_PER_NODE=1
fi
SNAPSHOT_INTERVAL=${SNAPSHOT_INTERVAL:-0} # seconds (disable periodic cuts to avoid race with barrier-cut)
export WINDOW_SIZE=${WINDOW_SIZE:-3600} # seconds (must match pump and WS calc)
AUTO_Y=${AUTO_Y:-0}
INTERACTIVE=${INTERACTIVE:-1}
ENABLE_PEBBLE_PHASE3=${ENABLE_PEBBLE_PHASE3:-1}
SNAPSHOT_FORMAT=${SNAPSHOT_FORMAT:-pebble}
# Parallel inflight replay: 0=auto (enable parallel), 1=sequential; default auto
INFLIGHT_WORKERS=${INFLIGHT_WORKERS:-0}
if [[ "$SNAPSHOT_FORMAT" != "pebble" ]]; then
  echo "demo_recovery: pebble-only mode; set SNAPSHOT_FORMAT=pebble" >&2
  exit 1
fi
PEBBLE_PHASE3_FLAG=""
if [[ "$ENABLE_PEBBLE_PHASE3" == "1" ]]; then
  PEBBLE_PHASE3_FLAG="--enable-pebble-phase3"
fi
# Ride-like pricing params (optional)
RIDE_BASE=${RIDE_BASE:-}
RIDE_PER_KM=${RIDE_PER_KM:-}
RIDE_DIST_MIN=${RIDE_DIST_MIN:-}
RIDE_DIST_MAX=${RIDE_DIST_MAX:-}
RIDE_SURGE_MIN=${RIDE_SURGE_MIN:-}
RIDE_SURGE_MAX=${RIDE_SURGE_MAX:-}
RIDE_CCY=${RIDE_CCY:-}
# Shutdown/view controls
NO_SHUTDOWN=${NO_SHUTDOWN:-0}                  # 1 = do NOT stop OpB at the end
SLEEP_BEFORE_SHUTDOWN=${SLEEP_BEFORE_SHUTDOWN:-30} # default 30s for non-interactive runs
VIEW_WAIT_SEC=${VIEW_WAIT_SEC:-0}              # extra seconds to wait after restart before proceeding
# Retries
CHECK_EXACT_RETRIES=${CHECK_EXACT_RETRIES:-30}  # seconds to wait for Exact mode to show up (reduced)
MANIFEST_OFFSETS_TIMEOUT=${MANIFEST_OFFSETS_TIMEOUT:-180}  # seconds to wait for manifest with offsets (barrier cut)
PRE_CUT_PRIME=${PRE_CUT_PRIME:-1}  # 1=prime small data before first full cut to ensure polling across partitions
CAUSAL_FREEZE_MODE=${CAUSAL_FREEZE_MODE:-0}
FREEZE_LAG_TIMEOUT=${FREEZE_LAG_TIMEOUT:-60}
FOCUS_INFLIGHT_DEMO=${FOCUS_INFLIGHT_DEMO:-1}

# High-level scenario presets for teaching/demo:
# - baseline: legacy pause-based recovery with Kafka replay
# - causal: causal inflight (Beaver-style), may still replay Kafka tail
# - freeze: causal snapshot freeze (epoch closed), aims for replay_s≈0
SCENARIO=${SCENARIO:-}
if [[ -n "$SCENARIO" ]]; then
  case "$SCENARIO" in
    baseline)
      CAUSAL_FREEZE_MODE=0
      # keep default POST_CUT_EVENTS to show replay tail
      ;;
    causal)
      CAUSAL_FREEZE_MODE=0
      ;;
    freeze)
      CAUSAL_FREEZE_MODE=1
      POST_CUT_EVENTS=0
      ;;
  esac
fi

wait_manifest_offsets() {
  local dir=${1:-$SNAPSHOT_DIR}
  local timeout=${2:-$MANIFEST_OFFSETS_TIMEOUT} # Default from env var, or 180s
  say "Waiting for manifest with partition offsets..."
  for ((i=1;i<=timeout;i++)); do
    if [[ -f "$dir/manifest.latest.json" ]] && jq -e '.changelog.offsets | length > 0' "$dir/manifest.latest.json" >/dev/null 2>&1; then
      say "✓ Manifest with offsets is ready."
      return 0
    fi
    sleep 1
  done
  say "WARN: Timed out waiting for manifest with offsets."
  return 1
}

wait_manifest_inflight() {
  local dir=${1:-$SNAPSHOT_DIR}
  local timeout=${2:-45}
  say "Waiting for manifest with inflight file..."
  for ((i=1;i<=timeout;i++)); do
    if [[ -f "$dir/manifest.latest.json" ]]; then
      inflight=$(jq -r '.inflightFile // ""' "$dir/manifest.latest.json" 2>/dev/null || echo "")
      if [[ -n "$inflight" && "$inflight" != "null" ]]; then
        MANIFEST_INFLIGHT_FILE="$inflight"
        MANIFEST_SNAPSHOT_ID=$(jq -r '.snapshotId // ""' "$dir/manifest.latest.json" 2>/dev/null || echo "")
        say "✓ Manifest has inflightFile."
        return 0
      fi
    fi
    sleep 1
  done
  say "WARN: Timed out waiting for inflight file."
  return 1
}

freeze_epoch_after_cut() {
  local manifest_file="$SNAPSHOT_DIR/manifest.latest.json"
  say "Freeze mode: sealing epoch for window ${WS:-unknown} (pausing ingestion, waiting lag=0)"
  curl -s -X POST "$OPB1_HTTP/admin/ingest/pause" >/dev/null || true
  local timeout=${FREEZE_LAG_TIMEOUT:-60}
  local lag=0
  for ((i=1;i<=timeout;i++)); do
    lag=$(get_status_field "lagTotal")
    printf "\r  [freeze %2d/%2d] lag=%s" "$i" "$timeout" "${lag:-unknown}"
    if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag == 0 )); then
      break
    fi
    sleep 1
  done
  printf "\n"
  if ! [[ "$lag" =~ ^[0-9]+$ ]] || (( lag != 0 )); then
    say "Freeze mode: WARN lag did not drain to 0 (last value=$lag)"
  else
    say "Freeze mode: lag drained to 0; marking manifest replayRequired=false"
  fi
  if [[ -f "$manifest_file" ]]; then
    local tmp
    tmp=$(mktemp) || true
    if [[ -n "$tmp" ]] && jq '.replayRequired=false' "$manifest_file" > "$tmp"; then
      mv "$tmp" "$manifest_file"
      say "Freeze mode: updated $manifest_file (replayRequired=false)"
    else
      say "Freeze mode: WARN unable to update $manifest_file"
      rm -f "$tmp" 2>/dev/null || true
    fi
  else
    say "Freeze mode: WARN manifest file not found at $manifest_file"
  fi
  if [[ -n "${MANIFEST_SNAPSHOT_ID:-}" ]]; then
    local archived="$SNAPSHOT_DIR/${MANIFEST_SNAPSHOT_ID}/manifest.json"
    if [[ -f "$archived" ]]; then
      local tmp2
      tmp2=$(mktemp) || true
      if [[ -n "$tmp2" ]] && jq '.replayRequired=false' "$archived" > "$tmp2"; then
        mv "$tmp2" "$archived"
        say "Freeze mode: updated archived manifest $archived"
      else
        say "Freeze mode: WARN unable to update archived manifest $archived"
        rm -f "$tmp2" 2>/dev/null || true
      fi
    fi
  fi
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

require_opbtool() {
  if [ ! -x "$BIN_OPBTOOL" ]; then
    say "Building opbtool helper..."
    go build -o "$BIN_OPBTOOL" ./cmd/opbtool
  fi
}

require_opb() {
  if [ ! -x "$BIN_OPB" ]; then
    say "Building opb binary..."
    go build -o "$BIN_OPB" ./cmd/opb || { say "ERROR: build opb failed"; exit 1; }
  fi
}

require_jq() {
  if ! command -v jq >/dev/null 2>&1; then
    say "ERROR: jq is required for parsing JSON. Please install jq (e.g., brew install jq) and retry."
    exit 1
  fi
}

# Poll /status to track causal cut progress and finalize
wait_causal_finalized() {
  local base=${1:-$OPB1_HTTP}
  local timeout=${2:-60}
  echo "[causal] waiting up to ${timeout}s..."
  for ((i=1;i<=timeout;i++)); do
    local j id seen tot phase
    j=$(curl -s "$base/status" || true)
    id=$(jq -r '.causalCutId // ""' <<<"$j")
    seen=$(jq -r '.causalMarkersSeen // 0' <<<"$j")
    tot=$(jq -r '.causalMarkersTotal // 0' <<<"$j")
    phase=$(jq -r '.causalPhase // ""' <<<"$j")
    printf "\r  [%2d] id=%s phase=%s markers=%s/%s" "$i" "${id:-nil}" "${phase:-nil}" "$seen" "$tot"
    if [[ "$id" != "" && "$tot" -gt 0 && "$seen" -eq "$tot" ]]; then
      echo; return 0
    fi
    sleep 1
  done
  echo; echo "[causal] WARN timeout"; return 1
}

# Parse restore phases line from logs and assert skip replay presence
parse_restore_phases() {
  local logf=${1:-$OPB1_LOG}
  grep -F "restore phases:" "$logf" | tail -n1 | sed -E 's/.*restore phases: //'
}

assert_skip_replay() {
  local logf=${1:-$OPB1_LOG}
  if grep -q "skipping changelog replay" "$logf"; then
    echo "[restore] ✓ skipped Kafka replay (no backlog)"
  else
    echo "[restore] info: Kafka replay executed (backlog existed)"
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

# Build optional extra pricing fields for ride-like pricing
build_extra_fields() {
  local extra=""
  if [[ -n "${RIDE_BASE}" ]]; then extra+=",\"fareBase\":${RIDE_BASE}"; fi
  if [[ -n "${RIDE_PER_KM}" ]]; then extra+=",\"farePerKm\":${RIDE_PER_KM}"; fi
  if [[ -n "${RIDE_DIST_MIN}" && -n "${RIDE_DIST_MAX}" ]]; then extra+=",\"distanceMinKm\":${RIDE_DIST_MIN},\"distanceMaxKm\":${RIDE_DIST_MAX}"; fi
  if [[ -n "${RIDE_SURGE_MIN}" && -n "${RIDE_SURGE_MAX}" ]]; then extra+=",\"surgeMin\":${RIDE_SURGE_MIN},\"surgeMax\":${RIDE_SURGE_MAX}"; fi
  if [[ -n "${RIDE_CCY}" ]]; then extra+=",\"currency\":\"${RIDE_CCY}\""; fi
  printf '%s' "$extra"
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

inject_delta_batch() {
  local total_base=$DELTA_BASE_EVENTS
  local extra_total=0
  local json_payload="["
  local extra_fields
  extra_fields=$(build_extra_fields)

  json_payload+="{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${DELTA_BASE_EVENTS},\"start\":1000,\"sync\":false${extra_fields}}"

  for store in "${DELTA_STORES[@]}"; do
    json_payload+=','
    json_payload+="{\"storeId\":\"$store\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${DELTA_EVENTS_PER_STORE},\"start\":0${extra_fields}}"
    extra_total=$((extra_total + DELTA_EVENTS_PER_STORE))
  done

  json_payload+=']'

  say "Injecting batch of jobs..."
  post_inject "$json_payload" >/dev/null

  DELTA_TOTAL_EVENTS=$total_base
  say "Injected $DELTA_TOTAL_EVENTS events for $STORE plus $extra_total events across ${#DELTA_STORES[@]} extra stores"
}

# Focused inflight demo: inject a small backlog only for the demo key (STORE, PROD, WS)
# This increases the chance that inflight.json will contain events exactly for ${STORE}#${PROD}#${WS}.
inject_demo_key_inflight() {
  local n=${1:-256}
  local extra_fields
  extra_fields=$(build_extra_fields)
  local payload="[{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${n},\"start\":900000,\"sync\":false${extra_fields}}]"
  say "Injecting focused inflight batch..."
  post_inject "$payload" >/dev/null
}

inject_post_cut_events() {
  local count=${1:-0}
  local start_ls=${2:-0}
  if [[ "$count" -le 0 ]]; then
    return
  fi
  say "Injecting $count post-cut events (after manifest) for $STORE"
  local payload="["
  local extra_fields
  extra_fields=$(build_extra_fields)
payload+="{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${count},\"start\":60000,\"sync\":false${extra_fields}}"
  payload+="]"
  RESP=$(post_inject "$payload")
  if command -v jq >/dev/null 2>&1; then echo "$RESP" | jq .; else echo "$RESP"; fi
  local target_ls=$(( start_ls + count ))
  say "Waiting for post-cut events to apply (target lastSeq=$target_ls)..."
  if wait_for_exact "$EXACT_URL_WS" "$target_ls" 60; then
    say "✓ Post-cut events applied (lastSeq reached $target_ls)"
  else
    say "WARN: post-cut events have not all applied (check logs)"
  fi
}

seed_baseline_state() {
  local count=${1:-0}
  local current_ls=${2:-0}
  if [[ "$count" -le 0 ]]; then
    return
  fi
  say "Seeding baseline state with $count events for $STORE (sync)"
  local payload="["
  local extra_fields
  extra_fields=$(build_extra_fields)
  payload+="{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${count},\"start\":0,\"sync\":false${extra_fields}}"
  payload+="]"
  RESP=$(post_inject "$payload")
  if command -v jq >/dev/null 2>&1; then echo "$RESP" | jq .; else echo "$RESP"; fi
  local target_ls=$(( current_ls + count ))
  if ! wait_for_exact "$EXACT_URL_WS" "$target_ls" "$CHECK_EXACT_RETRIES"; then
    say "WARN: baseline seed did not reach expected lastSeq=$target_ls (continuing)"
  fi
}

get_status_field() {
  local field=$1
  local base=${2:-$OPB1_HTTP}
  local data
  data=$(curl -s "$base/status" || true)
  if command -v jq >/dev/null 2>&1; then
    jq -r ".$field // 0" <<<"$data" 2>/dev/null || echo 0
  else
    printf '%s' "$data" | sed -n "s/.*\"$field\"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p" | head -n1
  fi
}

if [[ "$CAUSAL_FREEZE_MODE" == "1" ]]; then
  say "Causal Freeze mode enabled: disabling post-cut events and preparing replay-free manifest hints"
  POST_CUT_EVENTS=0
fi

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


ask_continue() {
  local msg=${1:-"Press y to continue, n to abort"}
  # Auto-continue switches
  if [[ "$AUTO_Y" == "1" || "$INTERACTIVE" == "0" ]]; then return 0; fi
  # Optional timeout (seconds). If set and no input within timeout, default to 'y'
  local timeout=${ASK_TIMEOUT:-0}
  local ans=""
  while true; do
    if [[ "$timeout" =~ ^[0-9]+$ ]] && (( timeout > 0 )); then
      read -r -t "$timeout" -p "${msg} [y/n]: " ans || { echo; echo "[demo] No input in ${timeout}s, defaulting to 'y'"; ans="y"; }
    else
      read -r -p "${msg} [y/n]: " ans
    fi
    case "$ans" in
      y|Y) return 0 ;;
      n|N) return 1 ;;
      *) echo "Please answer y or n." ;;
    esac
  done
}

wait_ready() {
  local url=$1; local n=${2:-60}
  say "Waiting for $url to be healthy (up to ${n}s)..."
  for((i=0;i<n;i++)); do if http_ok "$url"; then echo " OK"; return 0; fi; sleep 1; printf "."; done; echo " ERROR"; return 1
}

wait_assignment_count() {
  local expected=${1:-$EXPECTED_PARTITIONS}
  local timeout=${2:-60}
  say "Waiting for $expected partitions to be assigned to B1 (up to ${timeout}s)..."
  for ((i=1;i<=timeout;i++)); do
    local cnt
    cnt=$(curl -s "$OPB1_HTTP/status" | jq '.partitions | length' 2>/dev/null || echo 0)
    if [[ "$cnt" -eq "$expected" ]]; then
      say "✓ B1 partitions: $cnt"
      return 0
    fi
    sleep 1
  done
  say "WARN: B1 partition count did not reach $expected"
  return 1
}

wait_assignment_count_instance() {
  local name=${1:-B1}
  local base=${2:-$OPB1_HTTP}
  local expected=${3:-$EXPECTED_PARTITIONS_PER_INSTANCE}
  local timeout=${4:-120}
  say "Waiting for $name to hold $expected partitions (up to ${timeout}s)..."
  for ((i=1;i<=timeout;i++)); do
    local cnt
    cnt=$(curl -s "$base/status" | jq '.partitions | length' 2>/dev/null || echo 0)
    if [[ "$cnt" =~ ^[0-9]+$ ]] && (( cnt >= expected )); then
      say "✓ $name partitions: $cnt (>= $expected)"
      return 0
    fi
    sleep 1
  done
  say "WARN: $name partition count did not reach $expected"
  return 1
}

# --- Metrics & Visualization Helpers ---
METRICS_LOG=${METRICS_LOG:-./logs/demo_metrics.log}
mkdir -p "$(dirname "$METRICS_LOG")" 2>/dev/null || true
: > "$METRICS_LOG" 2>/dev/null || true
PROM_URL=${PROM_URL:-http://127.0.0.1:9090}
PROM_CHECK_TIMEOUT=${PROM_CHECK_TIMEOUT:-10}

log_metrics() {
  local msg=$1
  mkdir -p "$(dirname "$METRICS_LOG")" 2>/dev/null || true
  # Ghi metrics vào file riêng, không spam ra stdout
  printf '[metrics] %s\n' "$msg" >> "$METRICS_LOG"
}

prom_query_value() {
  local query=$1
  # Best-effort: never fail the demo if Prometheus is absent or returns error
  if [[ -z "${PROM_URL:-}" ]]; then
    echo ""
    return 0
  fi
  local resp
  resp=$(curl -sG "$PROM_URL/api/v1/query" --data-urlencode "query=$query" 2>/dev/null || true)
  if [[ -z "$resp" ]]; then
    echo ""
    return 0
  fi
  local status
  status=$(jq -r '.status // ""' <<<"$resp" 2>/dev/null || echo "")
  if [[ "$status" != "success" ]]; then
    echo ""
    return 0
  fi
  jq -r '.data.result[0].value[1]' <<<"$resp" 2>/dev/null || echo ""
}

log_prom_metric() {
  local label=$1 query=$2
  if [[ -z "${PROM_URL:-}" ]]; then
    log_metrics "[$label] WARN PROM_URL not set; skip query=$query"
    return
  fi
  local value
  value=$(prom_query_value "$query")
  if [[ -n "$value" && "$value" != "null" ]]; then
    log_metrics "[$label] prom query=$query value=$value"
  else
    log_metrics "[$label] WARN prometheus query returned empty (query=$query)"
  fi
}

check_prometheus_ready() {
  if [[ -z "${PROM_URL:-}" ]]; then
    say "WARN: PROM_URL is not set. Viz panel 'Causal inflight (last 5m)' will show errors; set PROM_URL or enter URL in the UI."
    return 0
  fi
  local health="${PROM_URL%/}/api/v1/status/config"
  say "Checking Prometheus availability at $PROM_URL ..."
  for ((i=1;i<=PROM_CHECK_TIMEOUT;i++)); do
    if curl -fsS "$health" >/dev/null 2>&1; then
      say "✓ Prometheus reachable (checked $PROM_URL)"
      return 0
    fi
    sleep 1
  done
  say "ERROR: Prometheus at $PROM_URL is not reachable after ${PROM_CHECK_TIMEOUT}s."
  say "  → Start Prometheus (or update PROM_URL) before re-running the recovery demo."
  return 1
}

log_status_endpoint() {
  local label=${1:-status} base=${2:-$OPB1_HTTP}
  local resp
  resp=$(curl -s "$base/status" 2>/dev/null || true)
  if [[ -z "$resp" ]]; then
    log_metrics "[$label] WARN empty /status response from $base"
    return
  fi
  if jq -e . >/dev/null 2>&1 <<<"$resp"; then
    local summary
    summary=$(jq -c '{status,instance,partitions:.partitions,lag:.lagTotal}' <<<"$resp" 2>/dev/null || echo "$resp")
    log_metrics "[$label] /status => $summary"
  else
    log_metrics "[$label] WARN non-JSON /status response (len=${#resp})"
  fi
}

log_causal_status() {
  local label=${1:-causal}
  local resp
  resp=$(curl -s "$OPB1_HTTP/status" 2>/dev/null || true)
  if [[ -z "$resp" ]]; then
    log_metrics "[$label] WARN empty causal status response"
    return
  fi
  if jq -e . >/dev/null 2>&1 <<<"$resp"; then
    local summary
    summary=$(jq -c '{cut:.causalCutId,phase:.causalPhase,seen:.causalMarkersSeen,total:.causalMarkersTotal,inflight:.causalInflight}' <<<"$resp" 2>/dev/null || echo "$resp")
    log_metrics "[$label] causal => $summary"
    log_prom_metric "${label}-prom-causal" "sum(opb_causal_inflight)"
  else
    log_metrics "[$label] WARN causal status not JSON (len=${#resp})"
  fi
}

log_cluster_viz() {
  local label=${1:-cluster} base=${2:-$OPB1_HTTP}
  local resp url="${base%/}/api/cluster"
  resp=$(curl -s "$url" 2>/dev/null || true)
  if [[ -z "$resp" ]]; then
    log_metrics "[$label] WARN empty /api/cluster response"
    return
  fi
  if jq -e . >/dev/null 2>&1 <<<"$resp"; then
    local summary
    summary=$(jq -c '{instances: (.instances // [] | map({id: (.instance // "unknown"), http: .http, parts: (.partitions // []), lag: (.lagTotal // 0)})), assignment: (.assignment // {})}' <<<"$resp" 2>/dev/null || echo "$resp")
    log_metrics "[$label] cluster => $summary"
  else
    log_metrics "[$label] WARN /api/cluster not JSON (len=${#resp})"
  fi
}

log_zone_viz() {
  local label=${1:-zone} store=${2:-$STORE}
  local url="$OPB1_HTTP/api/zone-details?id=$store"
  local resp
  resp=$(curl -s "$url" 2>/dev/null || true)
  if [[ -z "$resp" ]]; then
    log_metrics "[$label] WARN empty zone-data response"
    return
  fi
  if jq -e . >/dev/null 2>&1 <<<"$resp"; then
    local summary
    summary=$(jq -c '{storeId: (.storeId // .id // "'"$store"'"), sum: (.sumQty // .total // 0), cells: ((.cells // []) | length)}' <<<"$resp" 2>/dev/null || echo "$resp")
    log_metrics "[$label] zone-data => $summary"
  else
    log_metrics "[$label] WARN zone-data not JSON (len=${#resp})"
  fi
}

log_heatmap_viz() {
  local label=${1:-heatmap}
  local url="$OPB1_HTTP/viz/heatmap?metric=total&format=json"
  local resp
  resp=$(curl -s "$url" 2>/dev/null || true)
  if [[ -z "$resp" ]]; then
    log_metrics "[$label] WARN empty heatmap response"
    return
  fi
  if jq -e . >/dev/null 2>&1 <<<"$resp"; then
    local summary
    summary=$(jq -c '{metric: (.metric // "total"), cells: ((.cells // []) | length)}' <<<"$resp" 2>/dev/null || echo "$resp")
    log_metrics "[$label] heatmap => $summary"
  else
    log_metrics "[$label] WARN heatmap not JSON (len=${#resp})"
  fi
}

log_snapshot_metrics() {
  local label=${1:-snapshot} manifest=${2:-$SNAPSHOT_DIR/manifest.latest.json}
  if [[ ! -f "$manifest" ]]; then
    log_metrics "[$label] WARN manifest $manifest missing"
    return
  fi
  local sid type shards parent deltaSeq size="unknown"
  sid=$(jq -r '.snapshotId // ""' "$manifest" 2>/dev/null || echo "")
  if [[ -z "$sid" || "$sid" == "null" ]]; then
    log_metrics "[$label] WARN manifest missing snapshotId"
    return
  fi
  type=$(jq -r '.snapshotType // "full"' "$manifest" 2>/dev/null || echo "full")
  shards=$(jq -r '.shards // (.snapshotShards // 1)' "$manifest" 2>/dev/null || echo "")
  parent=$(jq -r '.parentId // ""' "$manifest" 2>/dev/null || echo "")
  deltaSeq=$(jq -r '.deltaSequence // ""' "$manifest" 2>/dev/null || echo "")
  local snapDir="$SNAPSHOT_DIR/$sid"
  if [[ -d "$snapDir" ]]; then
    size=$(du -sh "$snapDir" 2>/dev/null | awk '{print $1}')
  elif [[ -f "$snapDir/state.json" ]]; then
    size=$(du -sh "$snapDir/state.json" 2>/dev/null | awk '{print $1}')
  fi
  local keyCount
  keyCount=$(jq -r '.stats.keys // .keys // ""' "$manifest" 2>/dev/null || echo "")
  log_metrics "[$label] snapshot id=$sid type=$type shards=${shards:-?} keys=${keyCount:-unknown} size=${size:-unknown} parent=${parent:-none} deltaSeq=${deltaSeq:--}"
  log_prom_metric "${label}-prom-snapshot-bytes" "opb_snapshot_bytes"
}

# Verify Pebble manifest contains checksums for all SSTables
verify_pebble_manifest() {
  local manifest=${1:-$SNAPSHOT_DIR/manifest.latest.json}
  if [[ ! -f "$manifest" ]]; then
    say "WARN: manifest not found: $manifest"
    return 1
  fi
  local fmt
  fmt=$(jq -r '.snapshotFormat // ""' "$manifest" 2>/dev/null || echo "")
  if [[ "$fmt" != "pebble" ]]; then
    # Not a Pebble snapshot, skip verification
    return 0
  fi
  local files_count checksums_count
  files_count=$(jq -r '.pebbleSstFiles // [] | length' "$manifest" 2>/dev/null || echo 0)
  checksums_count=$(jq -r '.pebbleSstChecksums // {} | length' "$manifest" 2>/dev/null || echo 0)
  if [[ "$files_count" -eq 0 ]]; then
    say "WARN: Pebble manifest has no SSTable files listed"
    return 1
  fi
  if [[ "$checksums_count" -eq 0 ]]; then
    say "ERROR: Pebble manifest missing checksums (files=$files_count, checksums=0)"
    say "  → Checksums are required for integrity validation during restore"
    return 1
  fi
  if [[ "$checksums_count" -ne "$files_count" ]]; then
    say "WARN: Pebble manifest checksum count mismatch (files=$files_count, checksums=$checksums_count)"
    return 1
  fi
  say "✓ Pebble manifest verified: $files_count SSTables with $checksums_count checksums"
  # Show sample checksum for first file
  local first_file first_checksum
  first_file=$(jq -r '.pebbleSstFiles[0] // ""' "$manifest" 2>/dev/null || echo "")
  first_checksum=$(jq -r ".pebbleSstChecksums[\"$first_file\"] // \"\"" "$manifest" 2>/dev/null || echo "")
  if [[ -n "$first_file" && -n "$first_checksum" ]]; then
    say "  → Sample: $first_file → ${first_checksum:0:16}..."
  fi
  return 0
}

# Verify restore used Pebble shipping (not JSON load)
verify_pebble_restore() {
  local logf=${1:-$OPB1_LOG}
  if [[ "$SNAPSHOT_FORMAT" != "pebble" ]]; then
    return 0
  fi
  if grep -q "restore: restored Pebble snapshot" "$logf" 2>/dev/null; then
    local pebble_restore_line
    pebble_restore_line=$(grep "restore: restored Pebble snapshot" "$logf" | tail -n1)
    say "✓ Pebble SSTable shipping confirmed: $pebble_restore_line"
    local files_count sid
    files_count=$(echo "$pebble_restore_line" | sed -E 's/.*files=([0-9]+).*/\1/' || echo "")
    sid=$(echo "$pebble_restore_line" | sed -E 's/.*restored Pebble snapshot ([^ ]+).*/\1/' || echo "")
    if [[ -n "$files_count" && "$files_count" =~ ^[0-9]+$ ]]; then
      say "  → Imported $files_count SSTable files via atomic copy to STATE_DIR"
    fi
    if [[ -n "$sid" ]]; then
      say "  → Snapshot ID: $sid"
    fi
  elif grep -q "restore: snapshot restored" "$logf" 2>/dev/null; then
    local generic_line
    generic_line=$(grep "restore: snapshot restored" "$logf" | tail -n1)
    say "✓ Pebble snapshot restored: $generic_line"
    local sid
    sid=$(echo "$generic_line" | sed -E 's/.*snapshotId=([^ ]+).*/\1/' || echo "")
    if [[ -n "$sid" ]]; then
      say "  → Snapshot ID: $sid"
    fi
  else
    say "ERROR: Pebble restore not detected in logs"
    say "  Expected: 'restore: restored Pebble snapshot ...' or 'restore: snapshot restored ...'"
    say "  Check $logf for restore errors"
    return 1
  fi
  # Verify it did NOT use JSON load path
  if grep -q "restore: loaded.*keys from snapshot" "$logf" 2>/dev/null; then
    say "WARN: Both Pebble and JSON restore paths detected (unexpected)"
    return 1
  fi
  return 0
}

# Verify atomic import: STATE_DIR contains Pebble files (not snapshot dir)
verify_pebble_atomic_import() {
  if [[ "$SNAPSHOT_FORMAT" != "pebble" ]]; then
    return 0
  fi
  if [[ ! -d "$STATE_DIR" ]]; then
    say "WARN: STATE_DIR not found: $STATE_DIR"
    return 1
  fi
  # Check for Pebble files in STATE_DIR (not snapshot dir)
  local sst_count manifest_count current_log lock_file wal_count
  sst_count=$(find "$STATE_DIR" -maxdepth 1 -name "*.sst" 2>/dev/null | wc -l | tr -d ' ')
  manifest_count=$(find "$STATE_DIR" -maxdepth 1 -name "MANIFEST*" 2>/dev/null | wc -l | tr -d ' ')
  current_log=$(find "$STATE_DIR" -maxdepth 1 -name "CURRENT" 2>/dev/null | wc -l | tr -d ' ')
  lock_file=$(find "$STATE_DIR" -maxdepth 1 -name "LOCK" 2>/dev/null | wc -l | tr -d ' ')
  wal_count=$(find "$STATE_DIR" -maxdepth 1 -name "*.log" 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$manifest_count" -gt 0 && "$current_log" -gt 0 && ( "$sst_count" -gt 0 || "$wal_count" -gt 0 ) ]]; then
    say "✓ Atomic import verified: STATE_DIR contains Pebble DB files"
    say "  → SSTables: $sst_count, WAL logs: $wal_count, MANIFEST files: $manifest_count, CURRENT: $current_log"
    if [[ "$sst_count" -eq 0 ]]; then
      say "  → Note: SSTable count is 0 (small demo may reside entirely in WAL); manifest + WAL still prove import"
    fi
    say "  → Pebble DB opened at $STATE_DIR (not snapshot dir)"
    local state_size
    state_size=$(du -sh "$STATE_DIR" 2>/dev/null | awk '{print $1}' || echo "unknown")
    say "  → STATE_DIR size: $state_size"
    return 0
  fi
  say "WARN: Pebble files incomplete in STATE_DIR"
  say "  → SSTables: $sst_count (expected >0)"
  say "  → WAL logs: $wal_count (expected >=0)"
  say "  → MANIFEST: $manifest_count (expected >0)"
  say "  → CURRENT: $current_log (expected >0)"
  say "  → LOCK: $lock_file"
  return 1
}

verify_pebble_incremental() {
  local manifest=${1:-$SNAPSHOT_DIR/manifest.latest.json}
  if [[ "$ENABLE_PEBBLE_PHASE3" != "1" ]]; then
    return 0
  fi
  if [[ ! -f "$manifest" ]]; then
    say "WARN: incremental manifest not found: $manifest"
    return 1
  fi
  local inc_count all_count sid
  inc_count=$(jq -r '.pebbleIncrementalFiles // [] | length' "$manifest" 2>/dev/null || echo 0)
  all_count=$(jq -r '.pebbleAllFiles // [] | length' "$manifest" 2>/dev/null || echo 0)
  sid=$(jq -r '.snapshotId // ""' "$manifest" 2>/dev/null || echo "")
  if [[ "$inc_count" -le 0 ]]; then
    say "WARN: manifest $sid has no pebbleIncrementalFiles (Phase 3 expected)"
    return 1
  fi
  say "✓ Pebble incremental snapshot verified: $inc_count incremental SSTables referenced (total files=$all_count)"
  local first_inc
  first_inc=$(jq -r '.pebbleIncrementalFiles[0] // ""' "$manifest" 2>/dev/null || echo "")
  if [[ -n "$first_inc" && "$first_inc" != "null" ]]; then
    say "  → Sample incremental file: $first_inc"
  fi
  return 0
}

inspect_snapshot_with_opbtool() {
  local snapshot_id=$1
  if [[ -z "$snapshot_id" ]]; then
    say "WARN: snapshot ID missing for opbtool inspect"
    return 1
  fi
  require_opbtool
  mkdir -p ./logs 2>/dev/null || true
  local log_file="./logs/opbtool_${snapshot_id}.log"
  say "Inspecting snapshot $snapshot_id with opbtool (Phase 3 evidence)..."
  if "$BIN_OPBTOOL" -mode inspect -snapshot-dir "$SNAPSHOT_DIR" -snapshot-id "$snapshot_id" -keys "${OPBTOOL_KEYS:-5}" | tee "$log_file"; then
    say "✓ opbtool inspect complete (log: $log_file)"
  else
    say "WARN: opbtool inspect failed (see $log_file)"
    return 1
  fi
}

start_peer_instance() {
  say "Starting OpB peer (B2) for state migration test..."
  local peer_cmd=(
    "$BIN_OPB"
    --state-backend pebble --state-dir "${STATE_DIR}.b2" --snapshot-dir "$SNAPSHOT_DIR"
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID"
    --input-source kafka --topic-enriched "$ENRICHED_TOPIC"
    --changelog-dir "${CHANGELOG_DIR}.b2"
  --rebalance-import-state=true --peers "$OPB1_HTTP,$OPB3_HTTP"
    --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE"
    --topic-snapshots p1.opb-snapshots --topic-changelog p1.opb-changelog
    --manifest-sink both --manifest-source kafka --changelog-sink both --changelog-source kafka
  )
  if [[ -n "$PEBBLE_PHASE3_FLAG" ]]; then
    peer_cmd+=("$PEBBLE_PHASE3_FLAG")
  fi
  if [[ -n "${PROM_URL:-}" ]]; then
    peer_cmd+=( --prom-url "$PROM_URL" )
  fi
  peer_cmd+=( --http :8090 --instance-id B2 )
  "${peer_cmd[@]}" > "$OPB2_LOG" 2>&1 &
  OPB2_PID=$!
  if ! wait_ready "$OPB2_HTTP/healthz" 120; then
    say "ERROR: B2 failed to start"; tail -n 200 "$OPB2_LOG" || true; exit 1
  fi
}

stop_peer_instance() {
  if [[ -n "${OPB2_PID:-}" ]]; then
    say "Stopping peer B2 (PID=${OPB2_PID})"
    kill "$OPB2_PID" 2>/dev/null || true
    for i in {1..50}; do if ! kill -0 "$OPB2_PID" >/dev/null 2>&1; then break; fi; sleep 0.2; done
    unset OPB2_PID
  fi
}

start_third_instance() {
  say "Starting OpB peer (B3) for failure/recovery scenario..."
  local peer_cmd=(
    "$BIN_OPB"
    --state-backend pebble --state-dir "$STATE_DIR_B3" --snapshot-dir "$SNAPSHOT_DIR"
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID"
    --input-source kafka --topic-enriched "$ENRICHED_TOPIC"
    --changelog-dir "$CHANGELOG_DIR_B3"
  --rebalance-import-state=true --peers "$OPB1_HTTP,$OPB2_HTTP"
    --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE"
    --topic-snapshots p1.opb-snapshots --topic-changelog p1.opb-changelog
    --manifest-sink both --manifest-source kafka --changelog-sink both --changelog-source kafka
  )
  if [[ -n "$PEBBLE_PHASE3_FLAG" ]]; then
    peer_cmd+=("$PEBBLE_PHASE3_FLAG")
  fi
  if [[ -n "${PROM_URL:-}" ]]; then
    peer_cmd+=( --prom-url "$PROM_URL" )
  fi
  peer_cmd+=( --http "$OPB3_HTTP_ADDR" --instance-id B3 )
  "${peer_cmd[@]}" > "$OPB3_LOG" 2>&1 &
  OPB3_PID=$!
  if ! wait_ready "$OPB3_HTTP/healthz" 180; then
    say "ERROR: B3 failed to start"; tail -n 200 "$OPB3_LOG" || true; exit 1
  fi
}

stop_third_instance() {
  if [[ -n "${OPB3_PID:-}" ]]; then
    say "Stopping peer B3 (PID=${OPB3_PID})"
    kill "$OPB3_PID" 2>/dev/null || true
    for i in {1..50}; do if ! kill -0 "$OPB3_PID" >/dev/null 2>&1; then break; fi; sleep 0.2; done
    unset OPB3_PID
  fi
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
ensure_port_free 8090
ensure_port_free 8091

# --- Main Demo Logic ---

require_kadmin
require_jq

# Build opb binary if missing
require_opb

say "Phase 1: Setup & Create Snapshot"

say "Clean up old state, snapshots, and logs"
rm -rf "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR" "${STATE_DIR}.b2" "${CHANGELOG_DIR}.b2" "$STATE_DIR_B3" "$CHANGELOG_DIR_B3"
mkdir -p "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR" "${STATE_DIR}.b2" "${CHANGELOG_DIR}.b2" "$STATE_DIR_B3" "$CHANGELOG_DIR_B3" ./logs

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
  kadmin -cmd create -topic "$topic" -partitions "$ENRICHED_PARTITIONS" -rf 1 >/dev/null 2>&1 || true
}
ensure_enriched_topic "$ENRICHED_TOPIC"
reset_group_offsets "$GROUP_ID"

if ! check_prometheus_ready; then
  say "Stopping demo because Prometheus is required for viz panel coverage."
  exit 1
fi

# Ensure compacted topics exist for manifest and changelog (Kafka-mode)
ensure_compacted_topic() {
  local topic=$1
  say "Ensuring compacted topic exists: ${topic}"
  kadmin -cmd create -topic "$topic" -partitions "$SNAPSHOTS_PARTITIONS" -rf 1 -config "cleanup.policy=compact" >/dev/null 2>&1 || true
}
ensure_compacted_topic "p1.opb-snapshots"
# Changelog should NOT be compacted for delta replay; keep full history
ensure_delete_topic() {
  local topic=$1
  say "Ensuring delete-policy topic exists: ${topic}"
  kadmin -cmd create -topic "$topic" -partitions "$CHANGELOG_PARTITIONS" -rf 1 -config "cleanup.policy=delete" >/dev/null 2>&1 || true
}
ensure_delete_topic "p1.opb-changelog"

say "Start OpB (B1) with PebbleDB and snapshot interval=${SNAPSHOT_INTERVAL}s"
if [[ "$CAUSAL_FREEZE_MODE" == "1" ]]; then
  MANIFEST_SOURCE=${MANIFEST_SOURCE:-file}
else
  MANIFEST_SOURCE=${MANIFEST_SOURCE:-kafka}
fi
EXTRA_OPB_FLAGS=(--topic-snapshots p1.opb-snapshots --topic-changelog p1.opb-changelog --manifest-sink both --manifest-source ${MANIFEST_SOURCE} --changelog-sink both --changelog-source kafka --injector-linger-ms ${INJECTOR_LINGER_MS:-10})
if [[ -n "$PEBBLE_PHASE3_FLAG" ]]; then
  EXTRA_OPB_FLAGS+=("$PEBBLE_PHASE3_FLAG")
fi
if [[ -n "${PROM_URL:-}" ]]; then
  EXTRA_OPB_FLAGS+=( --prom-url "$PROM_URL" )
fi
OPB_CMD=(
  "$BIN_OPB"
  --state-backend pebble --state-dir "$STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR"
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID"
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC"
  --changelog-dir "$CHANGELOG_DIR"
  --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE"
  --snapshot-shards "${SNAPSHOT_SHARDS:-4}"
)
OPB_CMD+=( "${EXTRA_OPB_FLAGS[@]}" )
OPB_CMD+=( --peers "$OPB2_HTTP,$OPB3_HTTP" --http :8089 --instance-id B1 )
nohup "${OPB_CMD[@]}" > "$OPB1_LOG" 2>&1 < /dev/null &
OPB_PID=$!
disown || true

if ! wait_ready "$OPB1_HTTP/healthz" 180; then echo "ERROR: B1 failed to start"; tail -n 200 "$OPB1_LOG" || true; exit 1; fi
wait_assignment_count "$EXPECTED_PARTITIONS" 120

log_status_endpoint "b1-start-status" "$OPB1_HTTP"
log_cluster_viz "b1-start-cluster" "$OPB1_HTTP"
log_zone_viz "baseline-zone" "$STORE"
log_heatmap_viz "baseline-heatmap"

# Start B2 upfront to split partitions before heavy ingestion
say "Phase 1b: Start secondary OpB instance (B2) để chia partition"
start_peer_instance
wait_assignment_count_instance "B2" "$OPB2_HTTP" "$EXPECTED_PARTITIONS_PER_NODE" 180 || true
log_status_endpoint "b2-start-status" "$OPB2_HTTP"
log_cluster_viz "cluster-with-b2" "$OPB1_HTTP"

# Start B3 so cả cụm 3 node sẵn sàng trước seeding
say "Phase 1c: Start tertiary OpB instance (B3) để tăng throughput"
start_third_instance
say "Waiting for import logs from B3..."
import_b3=0
for ((i=1;i<=180;i++)); do
  if grep -q "import: finished loading" "$OPB3_LOG" 2>/dev/null; then
    say "✓ B3 import completed (see $OPB3_LOG)"
    import_b3=1
    break
  fi
  if grep -q "import: no data" "$OPB3_LOG" 2>/dev/null || grep -q "opb: ready" "$OPB3_LOG" 2>/dev/null; then
    say "✓ B3 has no state to import (ready)"
    import_b3=1
    break
  fi
  sleep 1
done
if [[ $import_b3 -ne 1 ]]; then
  say "WARN: Did not observe 'import: finished loading' in B3 logs (check $OPB3_LOG)"
fi
wait_assignment_count_instance "B3" "$OPB3_HTTP" "$EXPECTED_PARTITIONS_PER_NODE" 180 || true
log_status_endpoint "b3-start-status" "$OPB3_HTTP"
log_cluster_viz "cluster-with-b3" "$OPB1_HTTP"
say "B2 và B3 đã nhận partition (~${EXPECTED_PARTITIONS_PER_NODE} mỗi node). B1 vẫn giữ phần còn lại; bắt đầu seeding."

# Optional: seed via genorders Kafka mode (fast path, no HTTP rate limit)
# Always run GENORDERS_SEED unless explicitly disabled
if [[ "${GENORDERS_SEED:-1}" == "1" ]]; then
  say "GENORDERS_SEED: producing Kafka events via bin/genorders"
  if [ ! -x "./bin/genorders" ]; then
    say "Building genorders helper..."
    go build -o ./bin/genorders ./cmd/genorders || { say "ERROR: build genorders failed"; exit 1; }
  fi
  GEN_BOOT=${GEN_BOOTSTRAP:-$BOOTSTRAP}
  GEN_TOPIC=${GEN_TOPIC:-$ENRICHED_TOPIC}
  # Giảm seed mặc định để rút ngắn TTR; tăng qua env nếu cần tải lớn
  GEN_STORES=${GEN_STORES:-200}
  GEN_PRODUCTS=${GEN_PRODUCTS:-500}
  GEN_N=${GEN_N_PER_KEY:-1}
  GEN_WINDOW=${GEN_WINDOW_SIZE:-$WINDOW_SIZE}
  GEN_LINGER=${GEN_LINGER_MS:-10}
  ./bin/genorders --mode kafka \
    --bootstrap "$GEN_BOOT" --topic "$GEN_TOPIC" \
    --stores "$GEN_STORES" --products "$GEN_PRODUCTS" \
    --n-per-key "$GEN_N" --window-size "$GEN_WINDOW" --linger-ms "$GEN_LINGER"
  # Wait for lag to drain (default 120s)
  GEN_WAIT=${GEN_WAIT_LAG_SEC:-120}
  for ((i=1;i<=GEN_WAIT;i++)); do
    lag=$(get_lag_total)
    printf "\r[GENORDERS_SEED] waiting lag=%s (%d/%d)" "$lag" "$i" "$GEN_WAIT"
    if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= 1 )); then printf "\n"; break; fi
    sleep 1
  done

fi

# Optionally prime small data before the first full cut to ensure consumer polls all partitions
if [[ "${PRE_CUT_PRIME:-0}" == "1" ]]; then
  say "PRE_CUT_PRIME: producing a tiny amount of data to prime all partitions"
  if [ ! -x "./bin/genorders" ]; then
    say "Building genorders helper..."
    go build -o ./bin/genorders ./cmd/genorders || { say "ERROR: build genorders failed"; exit 1; }
  fi
  GEN_BOOT=${GEN_BOOTSTRAP:-$BOOTSTRAP}
  GEN_TOPIC=${GEN_TOPIC:-$ENRICHED_TOPIC}
  PRIME_STORES=${PRE_CUT_PRIME_STORES:-32}
  PRIME_PRODUCTS=${PRE_CUT_PRIME_PRODUCTS:-4}
  PRIME_N=${PRE_CUT_PRIME_N:-1}
  GEN_WINDOW=${GEN_WINDOW_SIZE:-$WINDOW_SIZE}
  GEN_LINGER=${GEN_LINGER_MS:-5}
  ./bin/genorders --mode kafka \
    --bootstrap "$GEN_BOOT" --topic "$GEN_TOPIC" \
    --stores "$PRIME_STORES" --products "$PRIME_PRODUCTS" \
    --n-per-key "$PRIME_N" --window-size "$GEN_WINDOW" --linger-ms "$GEN_LINGER"
  # Wait briefly for lag to drain and assignments to stabilize
  for ((i=1;i<=20;i++)); do
    lag=$(get_lag_total)
    printf "\r[PRE_CUT_PRIME] waiting lag=%s (%d/%d)" "$lag" "$i" 20
    if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= 1 )); then printf "\n"; break; fi
    sleep 1
  done
fi

# If backlog is very high after optional seeding, wait (bounded) before full cut to avoid 0/N markers for minutes
CUT_WAIT_LAG_THRESHOLD=${CUT_WAIT_LAG_THRESHOLD:-2000}
CUT_WAIT_LAG_SEC=${CUT_WAIT_LAG_SEC:-90}
lag=$(get_lag_total)
if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag > CUT_WAIT_LAG_THRESHOLD )); then
  say "Backlog is high (lag=$lag) — waiting up to ${CUT_WAIT_LAG_SEC}s for lag <= ${CUT_WAIT_LAG_THRESHOLD} before full cut"
  for ((i=1;i<=CUT_WAIT_LAG_SEC;i++)); do
    lag=$(get_lag_total)
    printf "\r  [pre-cut wait %3d/%d] lag=%s (threshold=%s)" "$i" "$CUT_WAIT_LAG_SEC" "$lag" "$CUT_WAIT_LAG_THRESHOLD"
    if [[ "$lag" =~ ^[0-9]+$ ]] && (( lag <= CUT_WAIT_LAG_THRESHOLD )); then printf "\n"; break; fi
    sleep 1
  done
  printf "\n"
fi

# Always trigger a barrier-based snapshot cut. This happens after the optional heavy seed and prime.
say "Triggering barrier-based snapshot cut..."
curl -s -X POST "$OPB1_HTTP/admin/snapshot-cut" | jq '.' || true
# If markers are stuck at 0/N shortly after cut, nudge consumer with tiny events
sleep 3
if command -v jq >/dev/null 2>&1; then
  SEEN=$(curl -s "$OPB1_HTTP/status" | jq -r '.causalMarkersSeen // 0')
  TOT=$(curl -s "$OPB1_HTTP/status" | jq -r '.causalMarkersTotal // 0')
else
  SEEN=0; TOT=0
fi
if [[ "$TOT" =~ ^[0-9]+$ ]] && [[ "$SEEN" =~ ^[0-9]+$ ]] && (( TOT > 0 )) && (( SEEN == 0 )); then
  say "Markers are 0/$TOT soon after cut; nudging consumer with tiny genorders batch"
  if [ ! -x "./bin/genorders" ]; then
    say "Building genorders helper..."
    go build -o ./bin/genorders ./cmd/genorders || { say "ERROR: build genorders failed"; exit 1; }
  fi
  ./bin/genorders --mode kafka --bootstrap "${GEN_BOOTSTRAP:-$BOOTSTRAP}" --topic "${GEN_TOPIC:-$ENRICHED_TOPIC}" \
    --stores "${NUDGE_STORES:-32}" --products "${NUDGE_PRODUCTS:-8}" --n-per-key "${NUDGE_N:-1}" \
    --window-size "${GEN_WINDOW_SIZE:-$WINDOW_SIZE}" --linger-ms "${NUDGE_LINGER_MS:-5}"
  sleep 2
fi

NOW=$(date +%s)
WS=$(( (NOW/ WINDOW_SIZE) * WINDOW_SIZE ))
USE_FUTURE_WINDOW=${USE_FUTURE_WINDOW:-0}
if [[ "$USE_FUTURE_WINDOW" == "1" ]]; then WS=$(( WS + WINDOW_SIZE )); fi
# Use store-total mode (no productId/ws) to match heatmap total aggregation
EXACT_URL="$OPB1_HTTP/api/zone-details?id=$STORE"
EXACT_URL_WS="$OPB1_HTTP/api/zone-details?id=$STORE&productId=$PROD&ws=$WS"
HEATMAP_URL="$OPB1_HTTP/viz/heatmap?metric=total"

say "Barrier mode: waiting for manifest with per-partition offsets before pumping delta"
wait_manifest_offsets "$SNAPSHOT_DIR" # Timeout: ${MANIFEST_OFFSETS_TIMEOUT}s (set MANIFEST_OFFSETS_TIMEOUT to override)
log_snapshot_metrics "full-snapshot" "$SNAPSHOT_DIR/manifest.latest.json"
verify_pebble_manifest "$SNAPSHOT_DIR/manifest.latest.json"
# Baseline before delta (optionally seed state for import demo)
base_sq=$(get_exact_sumqty "$EXACT_URL")
base_ls=$(get_lastseq "$EXACT_URL_WS")
if [[ "$BASELINE_EVENTS" -gt 0 ]]; then
  seed_baseline_state "$BASELINE_EVENTS" "$base_ls"
  base_sq=$(get_exact_sumqty "$EXACT_URL")
  base_ls=$(get_lastseq "$EXACT_URL_WS")
fi
say "Checkpoint 0: Baseline sumQty=$base_sq"

say "Phase 2: Validate peer instance B2 import"
  say "Waiting for import logs from B2..."
  import_logged=0
for ((i=1;i<=120;i++)); do
    if grep -q "import: finished loading" "$OPB2_LOG" 2>/dev/null; then
      say "✓ Peer import completed (see $OPB2_LOG)"
      import_logged=1
      break
    fi
  if grep -q "import: no data" "$OPB2_LOG" 2>/dev/null || grep -q "opb: ready" "$OPB2_LOG" 2>/dev/null; then
    say "✓ Peer has no state to import (ready)"
    import_logged=1
    break
  fi
    sleep 1
  done
  if [[ $import_logged -ne 1 ]]; then
    say "WARN: Did not observe 'import: finished loading' in B2 logs (check $OPB2_LOG)"
  fi
  log_status_endpoint "b2-import-status" "$OPB2_HTTP"
  log_cluster_viz "cluster-after-b2-import" "$OPB1_HTTP"
say "B2 will remain online through heavy seeding to increase throughput."

say "Phase 2: Create Delta Data with Causal Snapshot Capture"
say "=== Causal Snapshot Technique (Beaver-style) ==="

# Goal: Ensure messages are CONSUMED after cut begins but BEFORE barrier arrival.
# Strategy: Pause ingestion, inject events, trigger barrier cut, then immediately resume.
# This creates a race where the consumer starts processing the backlog just as barriers are propagating.

  say "Step 1: Pausing ingestion to build backlog"
  curl -s -X POST "$OPB1_HTTP/admin/ingest/pause" >/dev/null || true
  sleep 1

  say "Step 2: Injecting delta data to create backlog (while paused)"
  inject_delta_batch
  log_causal_status "delta-backlog-built"

if [[ "$FOCUS_INFLIGHT_DEMO" == "1" ]]; then
  inject_demo_key_inflight "${FOCUS_INFLIGHT_N:-256}"
fi

  say "Step 3: Triggering delta snapshot and resuming ingestion almost simultaneously"
  curl -s -X POST "$OPB1_HTTP/admin/snapshot-cut?type=delta" >/dev/null || true
  sleep 1 # Short delay to allow cut to initialize
  curl -s -X POST "$OPB1_HTTP/admin/ingest/resume" >/dev/null || true
  say "  Cut triggered and ingestion resumed. Inflight capture window is now open."

  log_causal_status "delta-after-resume"

# Track causal barrier progress via /status instead of grepping logs
wait_causal_finalized "$OPB1_HTTP" 120 || true
log_causal_status "delta-finalized"

# Technical explanation:
# 'currentCut' exists and currentCut.seen[partition]==false for all partitions.
# Backlogged messages will be consumed and recorded into inflight until each
# partition's barrier is encountered; then the cut finalizes and snapshot is taken.

expected_total=$(( base_sq + DELTA_TOTAL_EVENTS + POST_CUT_EVENTS ))
# Verification: Wait for manifest to contain inflightFile
# This confirms that the barrier cut completed and captured channel state
if wait_manifest_inflight "$SNAPSHOT_DIR" 60; then
  INFLIGHT_PATH="$SNAPSHOT_DIR/${MANIFEST_SNAPSHOT_ID:-}/$MANIFEST_INFLIGHT_FILE"
  log_snapshot_metrics "delta-snapshot" "$SNAPSHOT_DIR/manifest.latest.json"
  # Hiển thị snapshot-level vector clock (đa chiều) nếu có
  if command -v jq >/dev/null 2>&1; then
    SNAP_VC=$(jq -c '.vectorClock // {}' "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || echo "{}")
    say "Snapshot vectorClock: $SNAP_VC"
  fi
  verify_pebble_manifest "$SNAPSHOT_DIR/manifest.latest.json"
  verify_pebble_incremental "$SNAPSHOT_DIR/manifest.latest.json" || true
  if [[ -n "${MANIFEST_SNAPSHOT_ID:-}" ]]; then
    inspect_snapshot_with_opbtool "$MANIFEST_SNAPSHOT_ID"
  fi
  # Wait up to 15s for inflight.json to be materialized on disk (manifest may be published first)
  for ((i=1;i<=15;i++)); do
    if [[ -f "$INFLIGHT_PATH" ]]; then break; fi
    sleep 1
  done
  if [[ -f "$INFLIGHT_PATH" ]]; then
    if command -v jq >/dev/null 2>&1; then
      INFLIGHT_EVENT_COUNT=$(jq '(.events | map(length) | add) // 0' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
      INFLIGHT_CHANNELS=$(jq -r '.channels | length' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
        INFLIGHT_FOR_KEY=$(jq -r --arg k "${STORE}#${PROD}#${WS}" '
          (.events // {})
          | to_entries
          | map(.value | map(select(.key == $k)) | length)
          | add // 0
        ' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
      # Nếu key demo mặc định không có inflight nhưng snapshot có inflight cho key khác,
      # tự động chọn một key có inflight để dùng cho các checkpoint sau, tránh cần export STORE/PROD thủ công.
      if [[ "$INFLIGHT_FOR_KEY" -eq 0 && "$INFLIGHT_EVENT_COUNT" -gt 0 ]]; then
        auto_key=$(jq -r 'first(.events[] | .[0].key) // empty' "$INFLIGHT_PATH" 2>/dev/null || echo "")
        if [[ -n "$auto_key" ]]; then
          old_store="$STORE"; old_prod="$PROD"; old_ws="$WS"
          STORE="${auto_key%%#*}"
          rest="${auto_key#*#}"
          PROD="${rest%%#*}"
          WS="${rest##*#}"
          EXACT_URL="$OPB1_HTTP/api/zone-details?id=$STORE"
          EXACT_URL_WS="$OPB1_HTTP/api/zone-details?id=$STORE&productId=$PROD&ws=$WS"
          say "Auto-selected demo key from inflight: ${STORE}#${PROD}#${WS} (was ${old_store}#${old_prod}#${old_ws})"
          # Tính lại INFLIGHT_FOR_KEY cho key mới (để log đúng)
          INFLIGHT_FOR_KEY=$(jq -r --arg k "${STORE}#${PROD}#${WS}" '
            (.events // {})
            | to_entries
            | map(.value | map(select(.key == $k)) | length)
            | add // 0
          ' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
        fi
      fi
    else
      INFLIGHT_EVENT_COUNT=$(grep -c '"key"' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
      INFLIGHT_CHANNELS=0
        INFLIGHT_FOR_KEY=0
    fi
    say "✓ Causal snapshot captured: $INFLIGHT_EVENT_COUNT inflight events." 
    if [[ "$INFLIGHT_EVENT_COUNT" -gt 0 ]]; then
      say "  ✓ Channel state successfully captured - causal recovery will replay these events"
    else
      say "  WARN: Inflight file exists but contains 0 events"
      say "  This may indicate all messages were processed before barriers arrived"
    fi
  else
    say "WARN: inflight file $INFLIGHT_PATH not found (manifest references it but file missing)"
    # Best-effort diagnostics and fallback search
    say "  Listing snapshot dir: $SNAPSHOT_DIR/${MANIFEST_SNAPSHOT_ID:-}"
    ls -la "$SNAPSHOT_DIR/${MANIFEST_SNAPSHOT_ID:-}" 2>/dev/null || true
    alt_inflight=$(find "$SNAPSHOT_DIR" -maxdepth 2 -name "inflight.json" 2>/dev/null | head -n1 || true)
    if [[ -n "$alt_inflight" && -f "$alt_inflight" ]]; then
      say "  Found inflight.json at alternative path: $alt_inflight"
      INFLIGHT_PATH="$alt_inflight"
      if command -v jq >/dev/null 2>&1; then
        INFLIGHT_EVENT_COUNT=$(jq '(.events | map(length) | add) // 0' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
        INFLIGHT_CHANNELS=$(jq -r '.channels | length' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
      else
        INFLIGHT_EVENT_COUNT=$(grep -c '"key"' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
        INFLIGHT_CHANNELS=0
      fi
      say "✓ Causal snapshot captured (fallback): $INFLIGHT_EVENT_COUNT inflight events across $INFLIGHT_CHANNELS channels"
    else
      say "  inflight.json not found anywhere under $SNAPSHOT_DIR (will rely on changelog replay if needed)"
    fi
  fi
  if [[ "$CAUSAL_FREEZE_MODE" == "1" ]]; then
    freeze_epoch_after_cut
  fi
else
  say "WARN: inflightFile not observed in manifest within timeout"
  say "  This indicates barrier cut may not have captured channel state"
  say "  Causal recovery may be incomplete - check $OPB1_LOG for 'barrier-cut' messages"
fi

# Get current state before post-cut events
cur_sq=$(get_exact_sumqty "$EXACT_URL")
cur_ls=$(get_lastseq "$EXACT_URL_WS")
say "Checkpoint 2: Snapshot captured. Current sumQty=$cur_sq"
if [[ "$POST_CUT_EVENTS" -gt 0 ]]; then
  inject_post_cut_events "$POST_CUT_EVENTS" "$cur_ls"
  cur_sq=$(get_exact_sumqty "$EXACT_URL")
  cur_ls=$(get_lastseq "$EXACT_URL_WS")
  heatmap_val2=$(get_heatmap_total "$HEATMAP_URL" "$STORE")
  say "Post-cut checkpoint: heatmap=$heatmap_val2 sum=$cur_sq lastSeq=$cur_ls"
fi
PRE_CRASH_SUM=$cur_sq
PRE_CRASH_LASTSEQ=$cur_ls

# Ensure a manifest is actually published to Kafka before crashing, to avoid race at restart
wait_manifest_published() {
  local timeout=${1:-45}
  local i
  say "Waiting up to ${timeout}s for manifest publish (file check)..."
  for ((i=1;i<=timeout;i++)); do
    if [[ -f "$SNAPSHOT_DIR/manifest.latest.json" ]]; then
      local sid
      sid=$(jq -r '.snapshotId // ""' "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || echo "")
      if [[ -n "$sid" ]]; then
        MANIFEST_SNAPSHOT_ID="$sid"
        say "Manifest ready with snapshotId=$sid"
      return 0
      fi
    fi
    sleep 1
  done
  say "WARN: manifest file not observed within timeout; continuing anyway"
}
wait_manifest_published 45

say "Phase 3: Crash & Recovery (B3)"
if ! ask_continue "Phase 3: Ready to crash B3 now?"; then
  say "User chose to abort before crash — stopping demo."
  exit 0
fi
say "Inducing fault on B3 (kill -9)"
kill -9 "$OPB3_PID" || true
say "Waiting for process $OPB3_PID to fully exit..."
for i in {1..100}; do if ! kill -0 "$OPB3_PID" >/dev/null 2>&1; then break; fi; sleep 0.1; printf "."; done; echo
unset OPB3_PID

# Hiển thị phân bố partition ngay sau khi B3 down (trước khi restore)
say "Cluster assignment immediately after B3 crash (before restore):"
if command -v jq >/dev/null 2>&1; then
  curl -s "$OPB1_HTTP/api/cluster" | jq '.' || true
else
  curl -s "$OPB1_HTTP/api/cluster" || true
fi
if ! ask_continue "B3 đã down và B1/B2 đã nhận lại partition — tiếp tục chạy restore/hotspare cho B3?"; then
  say "User chose to inspect cluster state and stop before restore."
  exit 0
fi

CRASH_STATE_DIR="$STATE_DIR_B3"
CRASH_CHANGELOG_DIR="$CHANGELOG_DIR_B3"
CRASH_LOG="$OPB3_LOG"
CRASH_HTTP="$OPB3_HTTP"
CRASH_HTTP_ADDR="$OPB3_HTTP_ADDR"
CRASH_INSTANCE_ID="B3"

# Verify snapshot exists before recovery
say "Verifying snapshot exists before recovery..."
SNAPSHOT_ID=$(grep -o '"snapshotId":"[^"]*"' "$OPB1_LOG" 2>/dev/null | tail -n1 | sed 's/.*"snapshotId":"\([^"]*\)".*/\1/' || true)
if [[ -n "$SNAPSHOT_ID" ]]; then
  if [[ "$SNAPSHOT_FORMAT" == "pebble" ]]; then
    # Pebble snapshot: check for SSTable directory
    SNAPSHOT_DIR_PEBBLE="$SNAPSHOT_DIR/$SNAPSHOT_ID"
    if [[ -d "$SNAPSHOT_DIR_PEBBLE" ]]; then
      local sst_files
      sst_files=$(find "$SNAPSHOT_DIR_PEBBLE" -name "*.sst" 2>/dev/null | wc -l | tr -d ' ')
      SNAPSHOT_SIZE=$(du -sh "$SNAPSHOT_DIR_PEBBLE" 2>/dev/null | awk '{print $1}' || echo "unknown")
      say "Pebble snapshot found: $SNAPSHOT_ID ($sst_files SSTables, size: $SNAPSHOT_SIZE)"
    else
      say "WARN: Pebble snapshot directory not found: $SNAPSHOT_DIR_PEBBLE"
    fi
  else
    # JSON/msgpack snapshot: check for state.json
  SNAPSHOT_FILE="$SNAPSHOT_DIR/$SNAPSHOT_ID/state.json"
  if [[ -f "$SNAPSHOT_FILE" ]]; then
    SNAPSHOT_SIZE=$(stat -f%z "$SNAPSHOT_FILE" 2>/dev/null || stat -c%s "$SNAPSHOT_FILE" 2>/dev/null || echo "unknown")
    say "Snapshot found: $SNAPSHOT_ID (size: $SNAPSHOT_SIZE bytes)"
    else
      say "WARN: Snapshot file not found at $SNAPSHOT_FILE"
    fi
  fi
    if [[ -n "$MANIFEST_INFLIGHT_FILE" ]]; then
      INFLIGHT_PATH="$SNAPSHOT_DIR/$MANIFEST_INFLIGHT_FILE"
      if [[ -f "$INFLIGHT_PATH" ]]; then
        say "Inflight file located at $INFLIGHT_PATH"
      fi
  fi
else
  say "WARN: Could not extract snapshotId from logs"
fi

say "Restarting B3 in two stages..."

  say "Stage 1: Restore-only to rebuild state and exit (B3)"
  rm -f "$CRASH_STATE_DIR/LOCK" 2>/dev/null || true # Clean lock before restore
  RESTORE_CMD=(
    "$BIN_OPB"
    --state-backend pebble --state-dir "$CRASH_STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR"
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID"
    --input-source kafka --topic-enriched "$ENRICHED_TOPIC"
  )
  RESTORE_CMD+=( "${EXTRA_OPB_FLAGS[@]}" )
if [[ "$CAUSAL_FREEZE_MODE" == "1" ]]; then
  RESTORE_CMD+=( --restore-trust-manifest )
fi
  RESTORE_CMD+=( --window-size "$WINDOW_SIZE" --http :8092 --instance-id "$CRASH_INSTANCE_ID" --restore-on-start --restore-only )
  # Enable parallel inflight replay by default (0=auto)
  RESTORE_CMD+=( -replay-workers "$INFLIGHT_WORKERS" )
  if ! "${RESTORE_CMD[@]}" >> "$CRASH_LOG" 2>&1; then
    say "ERROR: restore-only failed — last 120 lines:"
    tail -n 120 "$CRASH_LOG" || true
    exit 1
  fi

  # Verify snapshot was restored
  say "Verifying snapshot restoration..."
  if [[ "$SNAPSHOT_FORMAT" == "pebble" ]]; then
    # Pebble backend: verify SSTable shipping was used
    verify_pebble_restore "$CRASH_LOG"
    # Verify atomic import into STATE_DIR
    ORIG_STATE_DIR="$STATE_DIR"
    STATE_DIR="$CRASH_STATE_DIR"
    verify_pebble_atomic_import
    STATE_DIR="$ORIG_STATE_DIR"
  else
    # JSON/msgpack backend: verify logical load
    if grep -q "restore: snapshot restored\|restore: loaded.*keys from snapshot" "$CRASH_LOG" 2>/dev/null; then
      RESTORED_SNAPSHOT=$(grep "restore: snapshot restored" "$CRASH_LOG" | tail -n1 | sed -E 's/.*snapshotId=([^ ]+).*/\1/' || echo "")
      if [[ -n "$RESTORED_SNAPSHOT" ]]; then
        say "✓ Snapshot restored successfully: $RESTORED_SNAPSHOT"
      fi
      # Extract number of keys loaded from snapshot (format: "restore: loaded X keys from snapshot Y")
      KEYS_LOADED=$(grep "restore: loaded.*keys from snapshot" "$CRASH_LOG" | tail -n1 | sed -E 's/.*loaded ([0-9]+) keys.*/\1/' || echo "")
      if [[ -n "$KEYS_LOADED" && "$KEYS_LOADED" =~ ^[0-9]+$ ]]; then
        say "✓ Loaded $KEYS_LOADED keys from snapshot"
      fi
    else
      say "WARN: Snapshot restoration log not found"
    fi
  fi

  # Report restore phases and whether changelog replay was skipped
  phases=$(parse_restore_phases "$CRASH_LOG" || true)
  if [[ -n "$phases" ]]; then
    say "restore phases: $phases"
  fi
  assert_skip_replay "$CRASH_LOG"

  # Verify causal replay: Check for inflight events being replayed
  if grep -q "inflight replay applied" "$CRASH_LOG" 2>/dev/null; then
    REPLAY_COUNT=$(grep "inflight replay applied" "$CRASH_LOG" | tail -n1 | sed -E 's/.*events=([0-9]+).*/\1/' 2>/dev/null || echo "")
    if [[ -n "$REPLAY_COUNT" && "$REPLAY_COUNT" =~ ^[0-9]+$ ]]; then
      say "✓ Causal inflight replay applied: $REPLAY_COUNT events replayed"
    else
      say "✓ Causal inflight replay detected in logs"
    fi
  else
    say "WARN: Causal replay log not detected (check $CRASH_LOG for 'inflight replay')"
    say "  If inflightFile was captured, this indicates a problem with causal recovery"
fi

say "Stage 2: Restart B3 to rejoin cluster"
RESTART_CMD=(
  "$BIN_OPB"
  --state-backend pebble --state-dir "$CRASH_STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR"
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID"
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC"
)
RESTART_CMD+=( "${EXTRA_OPB_FLAGS[@]}" )
if [[ "$CAUSAL_FREEZE_MODE" == "1" ]]; then
  RESTART_CMD+=( --restore-trust-manifest )
fi
RESTART_CMD+=( --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE" --restore-on-start --http "$CRASH_HTTP_ADDR" --instance-id "$CRASH_INSTANCE_ID" --skip-inflight-replay )
"${RESTART_CMD[@]}" >> "$CRASH_LOG" 2>&1 &
OPB3_PID2=$!

if ! wait_ready "$CRASH_HTTP/healthz" 180; then echo "ERROR: $CRASH_INSTANCE_ID failed to start after restore"; tail -n 400 "$CRASH_LOG" || true; exit 1; fi

log_status_endpoint "post-recovery-status-b3" "$OPB3_HTTP"
log_zone_viz "post-recovery-zone" "$STORE"
log_heatmap_viz "post-recovery-heatmap"

say "Phase 4: Verification"
# Với causal inflight hiện tại, mục tiêu là khôi phục lại đúng trạng thái trước crash.
# inflightForKey thể hiện số event đã được replay để rebuild PRE_CRASH_* từ snapshot,
# không phải phần delta cộng thêm trên PRE_CRASH_SUM.
EXPECTED_AFTER_SQ=$PRE_CRASH_SUM
EXPECTED_AFTER_LS=$PRE_CRASH_LASTSEQ
if wait_for_exact "$EXACT_URL_WS" "$EXPECTED_AFTER_LS" $CHECK_EXACT_RETRIES; then
  after_ls=$(get_lastseq "$EXACT_URL_WS"); after_sq=$(get_exact_sumqty "$EXACT_URL")
else
  after_ls=$(get_lastseq "$EXACT_URL_WS"); after_sq=$(get_exact_sumqty "$EXACT_URL")
fi
say "Checkpoint 3: State AFTER recovery"
say "Final sumQty=$after_sq (Expected=$EXPECTED_AFTER_SQ)"
# Wait for heatmap to reflect recovered state for STORE
wait_heatmap_value "$HEATMAP_URL" "$STORE" "$after_sq" 30
heatmap_val3=$(get_heatmap_total "$HEATMAP_URL" "$STORE")
say "✓ Heatmap Checkpoint 3: $heatmap_val3 (should reflect after-recovery total=$after_sq)"

# Per-key exactness: after should equal pre-crash + inflightForKey
if [[ -n "${PRE_CRASH_SUM:-}" ]]; then
  if [[ "$after_sq" -eq "$EXPECTED_AFTER_SQ" ]]; then
    say "✓ Exactness verified: Final sumQty matches expected value."
  else
    say "WARN: Per-key mismatch — Final sumQty ($after_sq) does not match expected ($EXPECTED_AFTER_SQ)."
  fi
fi

# Check causal metrics from /status endpoint
CAUSAL_REPLAY_TOTAL=$(get_status_field "causalReplayTotal" "$CRASH_HTTP")
CAUSAL_INFLIGHT_GAUGE=$(get_status_field "causalInflight" "$CRASH_HTTP")
say "Causal replay total: $CAUSAL_REPLAY_TOTAL events."
if [[ "$CAUSAL_REPLAY_TOTAL" -gt 0 ]]; then
  say "  ✓ Causal replay metric confirms events were replayed"
else
  say "  WARN: causalReplayTotal is 0 - no events were replayed (check if inflightFile was captured)"
fi



say "Recovery demo completed."

if [[ "$NO_SHUTDOWN" == "1" ]]; then
  say "NO_SHUTDOWN=1 set: leaving OpB running (B1 PID=${OPB_PID}, B2 PID=${OPB2_PID:-N/A}, B3 PID=${OPB3_PID2:-N/A})."; exit 0
fi

if [[ "$INTERACTIVE" == "1" ]]; then
  if ask_continue "Demo complete — stop all OpB processes now?"; then
    say "Stopping cluster per user confirmation..."
  else
    say "User chose to keep OpB running. Exiting without shutdown."
    exit 0
  fi
else
  if [[ "$SLEEP_BEFORE_SHUTDOWN" =~ ^[0-9]+$ ]] && [[ "$SLEEP_BEFORE_SHUTDOWN" -gt 0 ]]; then
    say "Sleeping ${SLEEP_BEFORE_SHUTDOWN}s before shutdown (SLEEP_BEFORE_SHUTDOWN)"; sleep "$SLEEP_BEFORE_SHUTDOWN"
  fi
fi

if [[ -n "${OPB3_PID2:-}" ]]; then
  say "Stopping OpB B3 (PID=${OPB3_PID2})..."
  kill "${OPB3_PID2}" 2>/dev/null || true
  for i in {1..50}; do if ! kill -0 "${OPB3_PID2}" >/dev/null 2>&1; then break; fi; sleep 0.1; done
fi
stop_peer_instance
say "Stopping OpB B1 (PID=${OPB_PID})..."; kill "${OPB_PID}" 2>/dev/null || true
for i in {1..50}; do if ! kill -0 "${OPB_PID}" >/dev/null 2>&1; then break; fi; sleep 0.1; done
say "Done."
