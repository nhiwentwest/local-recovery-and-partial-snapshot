#!/usr/bin/env bash
set -euo pipefail

# Baseline demo: Full snapshot restore + post-cut events => Kafka replay needed
# Purpose: produce Prometheus gauges (opb_last_restore_*) for A/B with partial/local recovery

# -------- Config --------
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
PROM_URL=${PROM_URL:-http://127.0.0.1:9090}
OPB1_HTTP=${OPB1_HTTP:-http://127.0.0.1:8089}
OPB2_HTTP=${OPB2_HTTP:-http://127.0.0.1:8090}
BIN_OPB=${BIN_OPB:-./bin/opb}
KADMIN_BIN=${KADMIN_BIN:-./bin/kadmin}
STATE_DIR=${STATE_DIR:-./data/baseline}
STATE_DIR_B2=${STATE_DIR_B2:-${STATE_DIR}.b2}
SNAPSHOT_DIR=${SNAPSHOT_DIR:-./snapshots-baseline}
CHANGELOG_DIR=${CHANGELOG_DIR:-./changelog-baseline}
ENRICHED_TOPIC=${ENRICHED_TOPIC:-p1.orders.enriched}
OUTPUT_TOPIC=${OUTPUT_TOPIC:-p1.orders.output}
TOPIC_SNAP=${TOPIC_SNAP:-p1.opb-snapshots}
TOPIC_CL=${TOPIC_CL:-p1.opb-changelog}
ENRICHED_PARTITIONS=${ENRICHED_PARTITIONS:-12}
CHANGELOG_PARTITIONS=${CHANGELOG_PARTITIONS:-4}
SNAPSHOTS_PARTITIONS=${SNAPSHOTS_PARTITIONS:-2}
WINDOW_SIZE=${WINDOW_SIZE:-3600}
GROUP_ID=${GROUP_ID:-opb-baseline-$(date +%s)}
GROUP_ID_B2=${GROUP_ID_B2:-${GROUP_ID}.b2}
INSTANCE1=${INSTANCE1:-B1}
INSTANCE2=${INSTANCE2:-B2}
POST_CUT_EVENTS=${POST_CUT_EVENTS:-150000}
SEED_STORES=${SEED_STORES:-600}
SEED_PRODUCTS=${SEED_PRODUCTS:-1500}
SEED_N_PER_KEY=${SEED_N_PER_KEY:-1}
INJECTOR_LINGER_MS=${INJECTOR_LINGER_MS:-10}
SNAPSHOT_INTERVAL=${SNAPSHOT_INTERVAL:-0}
ENABLE_PEBBLE_PHASE3=${ENABLE_PEBBLE_PHASE3:-0}
CUT_RETRY_MAX=${CUT_RETRY_MAX:-3}

say(){ printf "\n\e[1;35m[BASELINE]\e[0m %s\n" "$*"; }
http_ok(){ curl -sf "$1" >/dev/null 2>&1; }
require_kadmin(){ if [ ! -x "$KADMIN_BIN" ]; then say "Building kadmin..."; go build -o "$KADMIN_BIN" ./cmd/kadmin; fi }
require_opb(){ if [ ! -x "$BIN_OPB" ]; then say "Building opb..."; go build -o "$BIN_OPB" ./cmd/opb; fi }
ensure_port_free(){ local p=$1; if command -v lsof >/dev/null 2>&1; then local pid; pid=$(lsof -ti tcp:"$p" 2>/dev/null || true); [[ -n "$pid" ]] && { say "Kill pid $pid on :$p"; kill -9 $pid || true; sleep 1; }; fi; return 0; }
wait_ready(){ local url=$1; local n=${2:-180}; say "Waiting $url ..."; for((i=0;i<n;i++)); do http_ok "$url" && { echo OK; return 0; }; sleep 1; printf "."; done; echo ERR; return 1; }
get_lag(){ curl -s "$OPB1_HTTP/status" | sed -n 's/.*"lagTotal"[[:space:]]*:[[:space:]]*\([0-9.][0-9.]*\).*/\1/p' | head -n1; }

# -------- Prep --------
say "Kill any running opb on 8089/8090"
pkill -f "bin/opb" >/dev/null 2>&1 || true
sleep 1
ensure_port_free 8089
ensure_port_free 8090

say "Build binaries"
require_kadmin
require_opb

say "Clean dirs"
rm -rf "$STATE_DIR" "$STATE_DIR_B2" "$SNAPSHOT_DIR" "$CHANGELOG_DIR"
mkdir -p "$STATE_DIR" "$STATE_DIR_B2" "$SNAPSHOT_DIR" "$CHANGELOG_DIR" ./logs

say "Reset topics"
"$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd delete -topic "$ENRICHED_TOPIC" >/dev/null 2>&1 || true
"$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd create -topic "$ENRICHED_TOPIC" -partitions "$ENRICHED_PARTITIONS" -rf 1 >/dev/null 2>&1 || true
"$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd delete -topic "$TOPIC_SNAP" >/dev/null 2>&1 || true
"$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd create -topic "$TOPIC_SNAP" -partitions "$SNAPSHOTS_PARTITIONS" -rf 1 -config cleanup.policy=compact >/dev/null 2>&1 || true
"$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd delete -topic "$TOPIC_CL" >/dev/null 2>&1 || true
"$KADMIN_BIN" -bootstrap "$BOOTSTRAP" -cmd create -topic "$TOPIC_CL" -partitions "$CHANGELOG_PARTITIONS" -rf 1 -config cleanup.policy=delete >/dev/null 2>&1 || true

say "Start B1 (producer + manifest writer)"
OPB1_LOG=./logs/baseline_b1.log
"$BIN_OPB" \
  --state-backend pebble --state-dir "$STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
  --output-topic "$OUTPUT_TOPIC" \
  --changelog-dir "$CHANGELOG_DIR" \
  --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
  --manifest-sink both --manifest-source file --changelog-sink both --changelog-source kafka \
  --injector-linger-ms "$INJECTOR_LINGER_MS" \
  --snapshot-interval "$SNAPSHOT_INTERVAL" \
  ${ENABLE_PEBBLE_PHASE3:+--enable-pebble-phase3} \
  --http :8089 --instance-id "$INSTANCE1" > "$OPB1_LOG" 2>&1 &
B1_PID=$!
wait_ready "$OPB1_HTTP/healthz" 180 || { tail -n 120 "$OPB1_LOG" || true; exit 1; }

say "Seed baseline via genorders"
if [ ! -x ./bin/genorders ]; then go build -o ./bin/genorders ./cmd/genorders; fi
./bin/genorders --mode kafka --bootstrap "$BOOTSTRAP" --topic "$ENRICHED_TOPIC" \
  --stores "$SEED_STORES" --products "$SEED_PRODUCTS" --n-per-key "$SEED_N_PER_KEY" \
  --window-size "$WINDOW_SIZE" --linger-ms "$INJECTOR_LINGER_MS" --source baseline
# wait drain to near-zero before snapshot-cut (avoid inflight)
MAX_DRAIN_SEC=${MAX_DRAIN_SEC:-600}
for((i=1;i<=MAX_DRAIN_SEC;i++)); do
  lag=$(get_lag)
  printf "\r[seed] drain lag=%s (%d/%d s)" "$lag" "$i" "$MAX_DRAIN_SEC"
  if [[ "$lag" =~ ^[0-9.]+$ ]] && awk -v l="$lag" 'BEGIN{exit (l<=1)?0:1}'; then
    printf "\n"; break
  fi
  sleep 1
  if (( i % 60 == 0 )); then printf "\n"; fi
 done

# Cut full snapshot with retry until no inflightFile (do NOT pause consumer to allow barrier to finalize)
SNAP_OK=0
LAST_ID=""
for((attempt=1; attempt<=CUT_RETRY_MAX; attempt++)); do
  say "[cut $attempt/$CUT_RETRY_MAX] Full snapshot cut at lag≈0 (no inflight expected)"
  curl -s -X POST "$OPB1_HTTP/admin/snapshot-cut?type=full" >/dev/null || true
  say "Waiting for snapshot manifest..."
  for((i=1;i<=60;i++)); do [[ -f "$SNAPSHOT_DIR/manifest.latest.json" ]] && { break; }; sleep 1; done
  # wait for snapshot files
  READY=0
  for((i=1;i<=120;i++)); do
    SNAP_ID=$(cat "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null | jq -r '.snapshotId' 2>/dev/null || echo "")
    if [[ -n "$SNAP_ID" && "$SNAP_ID" != "null" ]]; then
      if [[ -d "$SNAPSHOT_DIR/$SNAP_ID" ]] && [[ -n "$(ls -A "$SNAPSHOT_DIR/$SNAP_ID" 2>/dev/null)" ]]; then
        READY=1; break
      fi
    fi
    sleep 1
  done
  [[ "$READY" == "1" ]] && say "Snapshot files ready: $SNAP_ID" || say "Timeout waiting files (attempt $attempt)"
  INFLIGHT=$(cat "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null | jq -r '.inflightFile // ""' 2>/dev/null || echo "")
  if [[ -z "$INFLIGHT" || "$INFLIGHT" == "null" ]]; then
    say "No inflightFile in manifest (baseline good)"
    SNAP_OK=1
    break
  else
    say "Manifest has inflightFile=$INFLIGHT — retrying cut after short idle"
    LAST_ID="$SNAP_ID"
    sleep 2
  fi
 done
if [[ "$SNAP_OK" != "1" ]]; then
  say "WARN: Could not obtain manifest without inflight after $CUT_RETRY_MAX attempts; continuing (baseline may include some inflight)."
fi

if [[ "$SNAP_OK" != "1" ]]; then
  say "WARN: Could not obtain manifest without inflight after $CUT_RETRY_MAX attempts; continuing (baseline may include some inflight)."
fi

POST_CUT_MODE=${POST_CUT_MODE:-genorders}
if [[ "$POST_CUT_MODE" == "genorders" ]]; then
  say "Post-cut via genorders: target N=$POST_CUT_EVENTS"
  if [ ! -x ./bin/genorders ]; then go build -o ./bin/genorders ./cmd/genorders; fi
  # Derive n-per-key so that stores*products*n-per-key >= POST_CUT_EVENTS
  PC_STORES=${PC_STORES:-$SEED_STORES}
  PC_PRODUCTS=${PC_PRODUCTS:-$SEED_PRODUCTS}
  denom=$(( PC_STORES * PC_PRODUCTS ))
  if (( denom <= 0 )); then denom=1; fi
  PC_N=$(( (POST_CUT_EVENTS + denom - 1) / denom ))
  (( PC_N <= 0 )) && PC_N=1
  say "genorders post-cut: stores=$PC_STORES products=$PC_PRODUCTS n-per-key=$PC_N"
  ./bin/genorders --mode kafka --bootstrap "$BOOTSTRAP" --topic "$ENRICHED_TOPIC" \
    --stores "$PC_STORES" --products "$PC_PRODUCTS" --n-per-key "$PC_N" \
    --window-size "$WINDOW_SIZE" --linger-ms "$INJECTOR_LINGER_MS" --source postcut
  DELAY_AFTER_GEN=${DELAY_AFTER_GEN:-15}
  say "Sleeping ${DELAY_AFTER_GEN}s to allow B1 to append changelog beyond manifest offsets"
  sleep "$DELAY_AFTER_GEN"
else
  say "Inject post-cut events via HTTP: N=$POST_CUT_EVENTS"
  NOW=$(date +%s); WS=$(( (NOW/ WINDOW_SIZE) * WINDOW_SIZE ))
  payload="[{\"storeId\":\"RECOVERY-BASE\",\"productId\":\"p1\",\"ws\":$WS,\"mode\":\"new\",\"n\":$POST_CUT_EVENTS,\"start\":1000,\"sync\":false}]"
  curl -s -X POST -H 'Content-Type: application/json' -d "$payload" "$OPB1_HTTP/api/inject-test-data" >/dev/null || true
  sleep 5
fi

# Proceed to start B2
DELAY_BEFORE_B2=${DELAY_BEFORE_B2:-10}
say "Starting B2 in ${DELAY_BEFORE_B2}s to capture backlog"
sleep "$DELAY_BEFORE_B2"
say "Starting B2 now (backlog should exist in Kafka for replay)"

say "Start B2 with --restore-on-start (will restore FULL and replay backlog), keep running"
OPB2_LOG=./logs/baseline_b2.log
PIN_DIR="${SNAPSHOT_DIR}.pin"
rm -rf "$PIN_DIR" && mkdir -p "$PIN_DIR"
cp "$SNAPSHOT_DIR/manifest.latest.json" "$PIN_DIR/manifest.latest.json" 2>/dev/null || true
if command -v jq >/dev/null 2>&1; then
  PIN_SNAP=$(jq -r '.snapshotId // ""' "$PIN_DIR/manifest.latest.json" 2>/dev/null || echo "")
else
  PIN_SNAP=""
fi
if [[ -n "$PIN_SNAP" && -d "$SNAPSHOT_DIR/$PIN_SNAP" ]]; then
  cp -R "$SNAPSHOT_DIR/$PIN_SNAP" "$PIN_DIR/" 2>/dev/null || true
fi
say "Pinned manifest+snapshot to $PIN_DIR (snapshotId=$PIN_SNAP)"
"$BIN_OPB" \
  --state-backend pebble --state-dir "$STATE_DIR_B2" --snapshot-dir "$PIN_DIR" \
  --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID_B2" \
  --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
  --changelog-dir "${CHANGELOG_DIR}.b2" \
  --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
  --manifest-sink both --manifest-source file --changelog-sink both --changelog-source kafka \
  --snapshot-interval 0 \
  --restore-on-start \
  ${ENABLE_PEBBLE_PHASE3:+--enable-pebble-phase3} \
  --http :8090 --instance-id "$INSTANCE2" > "$OPB2_LOG" 2>&1 &
B2_PID=$!
wait_ready "$OPB2_HTTP/healthz" 240 || { tail -n 200 "$OPB2_LOG" || true; exit 1; }

say "Wait for replay to drain (lag~0) so gauges reflect final TTR"
for((i=1;i<=300;i++)); do lag=$(get_lag); printf "\r[replay] lag=%s (%d/300)" "$lag" "$i"; [[ "$lag" =~ ^[0-9.]+$ ]] && awk -v l="$lag" 'BEGIN{exit (l<=1)?0:1}' && { printf "\n"; break; }; sleep 1; done

say "Done. Open /viz and set Prometheus URL to see 'Last Restore Summary'"
echo "- $OPB1_HTTP/viz/"
echo "If needed, Prometheus: $PROM_URL"
