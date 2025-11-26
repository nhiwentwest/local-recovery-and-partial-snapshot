#!/usr/bin/env bash
set -euo pipefail

# Local Recovery & Partial Snapshot Demo (Barrier-based Non-blocking Snapshot)
# Demonstrates recovery using partial snapshot with per-partition changelog offsets captured in manifest.

# --- Configurable Env Vars ---
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
OPB1_HTTP=${OPB1_HTTP:-http://127.0.0.1:8089}
OPB2_HTTP=${OPB2_HTTP:-http://127.0.0.1:8090}
BIN_OPB=${BIN_OPB:-./bin/opb}
KADMIN_BIN=${KADMIN_BIN:-./bin/kadmin}
STORE=${STORE:-RECOVERY-TEST}
PROD=${PROD:-p1}
STATE_DIR=./data/opb-recovery
SNAPSHOT_DIR=./snapshots-recovery
CHANGELOG_DIR=./changelog-recovery
OPB1_LOG=./logs/recovery_b1.out
OPB2_LOG=./logs/recovery_b2.out
MANIFEST_INFLIGHT_FILE=""
DELTA_STORES=("RECOVERY-A" "RECOVERY-B" "RECOVERY-C" "RECOVERY-D")
DELTA_EVENTS_PER_STORE=${DELTA_EVENTS_PER_STORE:-2500}
DELTA_BASE_EVENTS=${DELTA_BASE_EVENTS:-5000}
POST_CUT_EVENTS=${POST_CUT_EVENTS:-300}
BASELINE_EVENTS=${BASELINE_EVENTS:-512}
EXPECTED_PARTITIONS=${EXPECTED_PARTITIONS:-4}
SNAPSHOT_INTERVAL=${SNAPSHOT_INTERVAL:-0} # seconds (disable periodic cuts to avoid race with barrier-cut)
export WINDOW_SIZE=${WINDOW_SIZE:-3600} # seconds (must match pump and WS calc)
AUTO_Y=${AUTO_Y:-0}
INTERACTIVE=${INTERACTIVE:-1}
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

wait_manifest_offsets() {
  local dir=${1:-$SNAPSHOT_DIR}
  local timeout=${2:-45}
  say "Waiting up to ${timeout}s for manifest with per-partition offsets (barrier cut)..."
  for ((i=1;i<=timeout;i++)); do
    if [[ -f "$dir/manifest.latest.json" ]]; then
      local has
      has=$(jq -r '.changelog | if . != null and (.offsets|length) > 0 then "yes" else "no" end' "$dir/manifest.latest.json" 2>/dev/null || echo "no")
      if [[ "$has" == "yes" ]]; then
        local parts offs
        parts=$(jq -r '.changelog.partitions' "$dir/manifest.latest.json" 2>/dev/null || echo 0)
        offs=$(jq -c '.changelog.offsets' "$dir/manifest.latest.json" 2>/dev/null || echo [])
        say "✓ Manifest ready with per-partition offsets (parts=${parts})"
        echo "Offsets: ${offs}"
      return 0
      fi
    fi
    sleep 1
  done
  say "WARN: manifest with offsets not observed within timeout"
  return 1
}

wait_manifest_inflight() {
  local dir=${1:-$SNAPSHOT_DIR}
  local timeout=${2:-45}
  say "Waiting up to ${timeout}s for manifest containing inflightFile..."
  for ((i=1;i<=timeout;i++)); do
    if [[ -f "$dir/manifest.latest.json" ]]; then
      local inflight sid
      inflight=$(jq -r '.inflightFile // ""' "$dir/manifest.latest.json" 2>/dev/null || echo "")
      sid=$(jq -r '.snapshotId // ""' "$dir/manifest.latest.json" 2>/dev/null || echo "")
      if [[ -n "$inflight" && "$inflight" != "null" ]]; then
        MANIFEST_INFLIGHT_FILE="$inflight"
        if [[ -n "$sid" && "$sid" != "null" ]]; then
          MANIFEST_SNAPSHOT_ID="$sid"
        fi
        say "✓ Manifest has inflightFile=$inflight (snapshotId=${MANIFEST_SNAPSHOT_ID:-unknown})"
        return 0
      fi
    fi
    sleep 1
  done
  say "WARN: inflightFile not observed within timeout"
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

  json_payload+="{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${DELTA_BASE_EVENTS},\"start\":1000,\"sync\":true${extra_fields}}"

  for store in "${DELTA_STORES[@]}"; do
    json_payload+=','
    json_payload+="{\"storeId\":\"$store\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${DELTA_EVENTS_PER_STORE},\"start\":0${extra_fields}}"
    extra_total=$((extra_total + DELTA_EVENTS_PER_STORE))
  done

  json_payload+=']'

  say "Injecting batch of jobs..."
  RESP=$(post_inject "$json_payload")
  if command -v jq >/dev/null 2>&1; then echo "$RESP" | jq .; else echo "$RESP"; fi

  DELTA_TOTAL_EVENTS=$total_base
  say "Injected $DELTA_TOTAL_EVENTS events for $STORE plus $extra_total events across ${#DELTA_STORES[@]} extra stores"
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
  payload+="{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${count},\"start\":60000,\"sync\":true${extra_fields}}"
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
  payload+="{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":${count},\"start\":0,\"sync\":true${extra_fields}}"
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
  local data
  data=$(curl -s "$OPB1_HTTP/status" || true)
  if command -v jq >/dev/null 2>&1; then
    jq -r ".$field // 0" <<<"$data" 2>/dev/null || echo 0
  else
    printf '%s' "$data" | sed -n "s/.*\"$field\"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p" | head -n1
  fi
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

# --- Metrics & Visualization Helpers ---
METRICS_LOG=${METRICS_LOG:-./logs/demo_metrics.log}
mkdir -p "$(dirname "$METRICS_LOG")" 2>/dev/null || true
: > "$METRICS_LOG" 2>/dev/null || true
PROM_URL=${PROM_URL:-http://127.0.0.1:9090}

log_metrics() {
  local msg=$1
  mkdir -p "$(dirname "$METRICS_LOG")" 2>/dev/null || true
  printf '[metrics] %s\n' "$msg" | tee -a "$METRICS_LOG"
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

start_peer_instance() {
  say "Starting OpB peer (B2) for state migration test..."
  "$BIN_OPB" \
    --state-backend pebble --state-dir "${STATE_DIR}.b2" --snapshot-dir "$SNAPSHOT_DIR" \
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
    --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
    --changelog-dir "${CHANGELOG_DIR}.b2" \
    --rebalance-import-state=true --peers "$OPB1_HTTP" \
    --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE" \
    --topic-snapshots p1.opb-snapshots --topic-changelog p1.opb-changelog \
    --manifest-sink both --manifest-source kafka --changelog-sink both --changelog-source kafka \
    --http :8090 --instance-id B2 > "$OPB2_LOG" 2>&1 &
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

# --- Main Demo Logic ---

require_kadmin
require_jq

say "Phase 1: Setup & Create Snapshot"

say "Clean up old state, snapshots, and logs"
rm -rf "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR" "${STATE_DIR}.b2" "${CHANGELOG_DIR}.b2"
mkdir -p "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR" "${STATE_DIR}.b2" "${CHANGELOG_DIR}.b2" ./logs

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
EXTRA_OPB_FLAGS="--topic-snapshots p1.opb-snapshots --topic-changelog p1.opb-changelog --manifest-sink both --manifest-source kafka --changelog-sink both --changelog-source kafka --injector-linger-ms 100"
nohup "$BIN_OPB" \
  --state-backend pebble --state-dir "$STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
  --changelog-dir "$CHANGELOG_DIR" \
  --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE" \
  --snapshot-shards "${SNAPSHOT_SHARDS:-4}" \
  $EXTRA_OPB_FLAGS \
  --peers "$OPB2_HTTP" \
  --http :8089 --instance-id B1 > "$OPB1_LOG" 2>&1 < /dev/null &
OPB_PID=$!
disown || true

if ! wait_ready "$OPB1_HTTP/healthz" 180; then echo "ERROR: B1 failed to start"; tail -n 200 "$OPB1_LOG" || true; exit 1; fi
wait_assignment_count "$EXPECTED_PARTITIONS" 120

log_status_endpoint "b1-start-status" "$OPB1_HTTP"
log_cluster_viz "b1-start-cluster" "$OPB1_HTTP"
log_zone_viz "baseline-zone" "$STORE"
log_heatmap_viz "baseline-heatmap"

# Trigger a barrier-based snapshot cut to create a manifest with per-partition offsets
say "Triggering barrier-based snapshot cut..."
curl -s -X POST "$OPB1_HTTP/admin/snapshot-cut" | jq '.' || true

NOW=$(date +%s)
WS=$(( (NOW/ WINDOW_SIZE) * WINDOW_SIZE ))
USE_FUTURE_WINDOW=${USE_FUTURE_WINDOW:-0}
if [[ "$USE_FUTURE_WINDOW" == "1" ]]; then WS=$(( WS + WINDOW_SIZE )); fi
# Use store-total mode (no productId/ws) to match heatmap total aggregation
EXACT_URL="$OPB1_HTTP/api/zone-details?id=$STORE"
EXACT_URL_WS="$OPB1_HTTP/api/zone-details?id=$STORE&productId=$PROD&ws=$WS"
HEATMAP_URL="$OPB1_HTTP/viz/heatmap?metric=total"

say "Barrier mode: waiting for manifest with per-partition offsets before pumping delta"
wait_manifest_offsets "$SNAPSHOT_DIR" 45
log_snapshot_metrics "full-snapshot" "$SNAPSHOT_DIR/manifest.latest.json"
# Baseline before delta (optionally seed state for import demo)
base_sq=$(get_exact_sumqty "$EXACT_URL")
base_ls=$(get_lastseq "$EXACT_URL_WS")
if [[ "$BASELINE_EVENTS" -gt 0 ]]; then
  seed_baseline_state "$BASELINE_EVENTS" "$base_ls"
  base_sq=$(get_exact_sumqty "$EXACT_URL")
  base_ls=$(get_lastseq "$EXACT_URL_WS")
fi
say "Checkpoint 0: Baseline before delta"
echo "Exact URL (store-total): $EXACT_URL"
echo "Exact URL (window): $EXACT_URL_WS"
echo "Exact sumQty (total)=$base_sq lastSeq=$base_ls"

say "Phase 2: State migration (rebalance import)"
if ask_continue "Start peer instance B2 to demonstrate state migration?"; then
  start_peer_instance
  say "Waiting for import logs from B2..."
  import_logged=0
  for ((i=1;i<=60;i++)); do
    if grep -q "import: finished loading" "$OPB2_LOG" 2>/dev/null; then
      say "✓ Peer import completed (see $OPB2_LOG)"
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
  say "B2 is running; open $OPB2_HTTP/viz/cluster to observe assignment."
  if ask_continue "Stop peer instance B2 now?"; then
    stop_peer_instance
  else
    say "Keeping B2 running (remember to stop it manually after the demo)."
  fi
  sleep 5
  wait_assignment_count "$EXPECTED_PARTITIONS" 60
else
  say "Skipping peer migration demo"
fi

log_cluster_viz "post-b2-cluster" "$OPB1_HTTP"
say "Phase 2: Create Delta Data with Causal Snapshot Capture"
say "=== Causal Snapshot Technique (Beaver-style) ==="

# Goal: Ensure messages are CONSUMED after cut begins but BEFORE barrier arrival.
# We achieve this by pausing ingestion, creating backlog, injecting barriers, then resuming.

say "Step 1: Pausing ingestion to build backlog"
curl -s -X POST "$OPB1_HTTP/admin/ingest/pause" >/dev/null || true
sleep 0.5

say "Step 2: Injecting delta data to create backlog (while paused)"
inject_delta_batch
log_causal_status "delta-backlog-built"

say "Step 3: Triggering delta snapshot to initiate barrier cut (markers appended AFTER backlog)"
curl -s -X POST "$OPB1_HTTP/admin/snapshot-cut?type=delta" >/dev/null || true

# Wait for barrier injection to complete (check log for exact pattern)
say "Step 4: Waiting for barrier messages to be injected into Kafka..."
barrier_injected=0
for ((i=1;i<=20;i++)); do
  if grep -q "snapshot-cut: barrier injected" "$OPB1_LOG" 2>/dev/null; then
    say "✓ Barrier messages injected and flushed to Kafka (confirmed in logs)"
    barrier_injected=1
    break
  fi
  sleep 0.2
done
if [[ $barrier_injected -eq 0 ]]; then
  say "WARN: Barrier injection not confirmed in logs, proceeding anyway"
  say "  (Check $OPB1_LOG for 'snapshot-cut: barrier injected')"
fi

say "Step 5: Resuming ingestion so backlog flows and gets captured as inflight"
curl -s -X POST "$OPB1_HTTP/admin/ingest/resume" >/dev/null || true
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
  if [[ -f "$INFLIGHT_PATH" ]]; then
    if command -v jq >/dev/null 2>&1; then
      INFLIGHT_EVENT_COUNT=$(jq '(.events | map(length) | add) // 0' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
      INFLIGHT_CHANNELS=$(jq -r '.channels | length' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
    else
      INFLIGHT_EVENT_COUNT=$(grep -c '"key"' "$INFLIGHT_PATH" 2>/dev/null || echo 0)
      INFLIGHT_CHANNELS=0
    fi
    say "✓ Causal snapshot captured: $INFLIGHT_EVENT_COUNT inflight events across $INFLIGHT_CHANNELS channels"
    say "  File: $MANIFEST_INFLIGHT_FILE"
    if [[ "$INFLIGHT_EVENT_COUNT" -gt 0 ]]; then
      say "  ✓ Channel state successfully captured - causal recovery will replay these events"
    else
      say "  WARN: Inflight file exists but contains 0 events"
      say "  This may indicate all messages were processed before barriers arrived"
    fi
  else
    say "WARN: inflight file $INFLIGHT_PATH not found (manifest references it but file missing)"
  fi
else
  say "WARN: inflightFile not observed in manifest within timeout"
  say "  This indicates barrier cut may not have captured channel state"
  say "  Causal recovery may be incomplete - check $OPB1_LOG for 'barrier-cut' messages"
fi

say "Checkpoint 2: Snapshot captured; recording current stats"
cur_sq=$(get_exact_sumqty "$EXACT_URL")
cur_ls=$(get_lastseq "$EXACT_URL_WS")
heatmap_val2=$(get_heatmap_total "$HEATMAP_URL" "$STORE")
say "Heatmap Checkpoint 2: $heatmap_val2 (current sum=${cur_sq}, expected eventual sum=$expected_total)"
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

ask_continue "Ready to crash the instance?" || { echo Aborted; exit 1; }

say "Phase 3: Crash & Recovery"
say "Inducing fault (kill -9)"
kill -9 "$OPB_PID" || true
say "Waiting for process $OPB_PID to fully exit..."
for i in {1..100}; do if ! kill -0 "$OPB_PID" >/dev/null 2>&1; then break; fi; sleep 0.1; printf "."; done; echo

# Verify snapshot exists before recovery
say "Verifying snapshot exists before recovery..."
SNAPSHOT_ID=$(grep -o '"snapshotId":"[^"]*"' "$OPB1_LOG" 2>/dev/null | tail -n1 | sed 's/.*"snapshotId":"\([^"]*\)".*/\1/' || true)
if [[ -n "$SNAPSHOT_ID" ]]; then
  SNAPSHOT_FILE="$SNAPSHOT_DIR/$SNAPSHOT_ID/state.json"
  if [[ -f "$SNAPSHOT_FILE" ]]; then
    SNAPSHOT_SIZE=$(stat -f%z "$SNAPSHOT_FILE" 2>/dev/null || stat -c%s "$SNAPSHOT_FILE" 2>/dev/null || echo "unknown")
    say "Snapshot found: $SNAPSHOT_ID (size: $SNAPSHOT_SIZE bytes)"
    if [[ -n "$MANIFEST_INFLIGHT_FILE" ]]; then
      INFLIGHT_PATH="$SNAPSHOT_DIR/$MANIFEST_INFLIGHT_FILE"
      if [[ -f "$INFLIGHT_PATH" ]]; then
        say "Inflight file located at $INFLIGHT_PATH"
      fi
    fi
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
  --restore-on-start --restore-only >> "$OPB1_LOG" 2>&1

# Verify snapshot was restored
say "Verifying snapshot restoration..."
if grep -q "restore: snapshot restored" "$OPB1_LOG" 2>/dev/null; then
  RESTORED_SNAPSHOT=$(grep "restore: snapshot restored" "$OPB1_LOG" | tail -n1 | sed -E 's/.*snapshotId=([^ ]+).*/\1/' || echo "")
  if [[ -n "$RESTORED_SNAPSHOT" ]]; then
    say "✓ Snapshot restored successfully: $RESTORED_SNAPSHOT"
  fi
  # Extract number of keys loaded from snapshot (format: "restore: loaded X keys from snapshot Y")
  KEYS_LOADED=$(grep "restore: loaded.*keys from snapshot" "$OPB1_LOG" | tail -n1 | sed -E 's/.*loaded ([0-9]+) keys.*/\1/' || echo "")
  if [[ -n "$KEYS_LOADED" && "$KEYS_LOADED" =~ ^[0-9]+$ ]]; then
    say "✓ Loaded $KEYS_LOADED keys from snapshot"
  fi
else
  say "WARN: Snapshot restoration log not found"
fi

# Report restore phases and whether changelog replay was skipped
phases=$(parse_restore_phases "$OPB1_LOG" || true)
if [[ -n "$phases" ]]; then
  say "restore phases: $phases"
fi
assert_skip_replay "$OPB1_LOG"

# Verify causal replay: Check for inflight events being replayed
if grep -q "inflight replay applied" "$OPB1_LOG" 2>/dev/null; then
  REPLAY_COUNT=$(grep "inflight replay applied" "$OPB1_LOG" | tail -n1 | sed -E 's/.*events=([0-9]+).*/\1/' 2>/dev/null || echo "")
  if [[ -n "$REPLAY_COUNT" && "$REPLAY_COUNT" =~ ^[0-9]+$ ]]; then
    say "✓ Causal inflight replay applied: $REPLAY_COUNT events replayed"
  else
    say "✓ Causal inflight replay detected in logs"
  fi
else
  say "WARN: Causal replay log not detected (check $OPB1_LOG for 'inflight replay')"
  say "  If inflightFile was captured, this indicates a problem with causal recovery"
fi

say "Stage 2: Start normally to begin consuming"
"$BIN_OPB" \
  --state-backend pebble --state-dir "$STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
  $EXTRA_OPB_FLAGS \
  --snapshot-interval "$SNAPSHOT_INTERVAL" --window-size "$WINDOW_SIZE" \
  --restore-on-start \
  --http :8089 --instance-id B1 >> "$OPB1_LOG" 2>&1 &
OPB_PID2=$!

if ! wait_ready "$OPB1_HTTP/healthz" 180; then echo "ERROR: B1 failed to start after restore"; tail -n 400 "$OPB1_LOG" || true; exit 1; fi

log_status_endpoint "post-recovery-status" "$OPB1_HTTP"
log_cluster_viz "post-recovery-cluster" "$OPB1_HTTP"
log_zone_viz "post-recovery-zone" "$STORE"
log_heatmap_viz "post-recovery-heatmap"

say "Phase 4: Verification"
if wait_for_exact "$EXACT_URL_WS" "$PRE_CRASH_LASTSEQ" $CHECK_EXACT_RETRIES; then
  after_ls=$(get_lastseq "$EXACT_URL_WS"); after_sq=$(get_exact_sumqty "$EXACT_URL")
else
  after_ls=$(get_lastseq "$EXACT_URL_WS"); after_sq=$(get_exact_sumqty "$EXACT_URL")
fi
say "Checkpoint 3: State AFTER recovery"
echo "Exact URL (store-total): $EXACT_URL"
echo "Exact URL (window): $EXACT_URL_WS"
echo "Exact sumQty (total, after)=$after_sq lastSeq(after)=$after_ls (expected lastSeq>=$cur_ls and sumQty==$cur_sq)"
# Wait for heatmap to reflect recovered state (should match pre-crash value)
wait_heatmap_value "$HEATMAP_URL" "$STORE" "$after_sq" 30
heatmap_val3=$(get_heatmap_total "$HEATMAP_URL" "$STORE")
say "✓ Heatmap Checkpoint 3: $heatmap_val3 (expected ~$after_sq, should match pre-crash ~$PRE_CRASH_SUM)"

# Verify causal consistency: SumQty should match pre-crash value
# This proves that inflight events were correctly replayed during recovery
if [[ -n "${PRE_CRASH_SUM:-}" ]]; then
  if [[ "$after_sq" -eq "$PRE_CRASH_SUM" ]]; then
    say "✓ Causal consistency verified: SumQty after recovery ($after_sq) matches pre-crash ($PRE_CRASH_SUM)"
    say "  This confirms that inflight events were correctly replayed"
  else
    say "WARN: SumQty mismatch - after recovery ($after_sq) vs pre-crash ($PRE_CRASH_SUM)"
    say "  Difference: $(( after_sq - PRE_CRASH_SUM ))"
    say "  This may indicate:"
    say "    - Inflight events were not captured (check inflightFile in manifest)"
    say "    - Inflight events were not replayed (check logs for 'inflight replay')"
    say "    - Some events were processed after barrier but before crash"
  fi
fi

# Check causal metrics from /status endpoint
CAUSAL_REPLAY_TOTAL=$(get_status_field "causalReplayTotal")
CAUSAL_INFLIGHT_GAUGE=$(get_status_field "causalInflight")
say "Causal metrics from /status:"
say "  - causalReplayTotal: $CAUSAL_REPLAY_TOTAL (events replayed during restore)"
say "  - causalInflight: $CAUSAL_INFLIGHT_GAUGE (current inflight events, should be 0 after recovery)"
if [[ "$CAUSAL_REPLAY_TOTAL" -gt 0 ]]; then
  say "  ✓ Causal replay metric confirms events were replayed"
else
  say "  WARN: causalReplayTotal is 0 - no events were replayed (check if inflightFile was captured)"
fi

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
