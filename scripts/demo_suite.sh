#!/usr/bin/env bash
set -euo pipefail

# Guided demo suite with prompts. Works in zsh/IDE by reading /dev/tty or auto-continue when not interactive.
# Demos:
# 1) EOS (new vs duplicate for an exact key)
# 2) Scale-out (start a second OpB instance; verify instances in zone data)
# 3) Throughput/Latency (simple metrics delta and exact latency measurement)
# Controls:
# - AUTO_Y=1          -> always continue without prompts
# - INTERACTIVE=0     -> show a short countdown and continue (for IDEs without TTY)
# - DEMO_ONLY=EOS     -> run EOS part only and exit

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
OPA_HTTP=${OPA_HTTP:-http://127.0.0.1:8088}
OPB1_HTTP=${OPB1_HTTP:-http://127.0.0.1:8089}
OPB2_HTTP=${OPB2_HTTP:-http://127.0.0.1:8090}
PROM_HTTP=${PROM_HTTP:-http://127.0.0.1:9090}
GROUP_ID=${GROUP_ID:-opb}
STATE2_DIR=${STATE2_DIR:-./data/opb-standalone-2}
SNAPSHOT_DIR=${SNAPSHOT_DIR:-./snapshots}
INSTANCE2_ID=${INSTANCE2_ID:-B2}
BIN_OPB=${BIN_OPB:-./bin/opb}
AUTO_Y=${AUTO_Y:-0}
INTERACTIVE=${INTERACTIVE:-1}
DEMO_ONLY=${DEMO_ONLY:-}
EOS_WINDOW_SIZE=${EOS_WINDOW_SIZE:-3600}

# If interactive prompts are desired but stdin isn't a TTY, bind stdin to /dev/tty once for the whole script
if [[ "$AUTO_Y" != "1" && "$INTERACTIVE" != "0" && ! -t 0 && -e /dev/tty ]]; then
  exec </dev/tty
fi

say() { printf "\n\e[1;36m[DEMO]\e[0m %s\n" "$*"; }
countdown_continue() {
  local msg=${1:-"Continuing"}
  local secs=${2:-6}
  for ((i=secs;i>0;i--)); do printf "\r%s (auto-continue in %ds)" "$msg" "$i"; sleep 1; done; echo
}
ask_continue() {
  local msg=${1:-"Press y to continue, n to abort"}
  if [[ "$AUTO_Y" == "1" ]]; then return 0; fi
  if [[ "$INTERACTIVE" == "0" ]]; then countdown_continue "$msg" 6; return 0; fi
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
need() { command -v "$1" >/dev/null 2>&1 || { echo "Need $1"; exit 1; }; }
ws_now() {
  python3 - "$EOS_WINDOW_SIZE" <<'PY'
import sys
import time
win = 3600
if len(sys.argv) > 1:
    try:
        val = int(sys.argv[1])
        if val > 0:
            win = val
    except Exception:
        pass
now = int(time.time())
print((now // win) * win)
PY
}
http_ok() { curl -sf "$1" >/dev/null 2>&1; }
http_code() { curl -s -o /dev/null -w "%{http_code}" "$1"; }
metric_val() { curl -s "$1/metrics" | awk -v name="$2" '$1==name{print $2}' | tail -1; }
get_sumqty_exact() {
  local url=$1
  # avoid pipefail aborts: capture curl status and fallback to {}
  local body
  if ! body=$(curl -sfG "$url" 2>/dev/null); then
    echo 0; return 0
  fi
  python3 - "$body" <<'PY'
import json
import sys
body = sys.argv[1] if len(sys.argv) > 1 else "{}"
try:
    data = json.loads(body)
    print(int(data.get("sumQty", 0)))
except Exception:
    print(0)
PY
}

# Requirements (curl, python3 needed for JSON parsing)
need curl
need python3

# --- Prometheus helpers ---
urlencode() {
  python3 - "$1" <<'PY'
import sys, urllib.parse
s = sys.argv[1] if len(sys.argv)>1 else ''
print(urllib.parse.quote(s, safe=''))
PY
}
prom_val() {
  local q=$1
  local uq; uq=$(urlencode "$q")
  local body
  if ! body=$(curl -sf "$PROM_HTTP/api/v1/query?query=$uq" 2>/dev/null); then echo 0; return 0; fi
  python3 - "$body" <<'PY'
import json,sys
try:
  data=json.loads(sys.argv[1]); r=data.get('data',{}).get('result',[])
  if not r: print(0)
  else: print(r[0]['value'][1])
except: print(0)
PY
}
# Average throughput over last N seconds via increase(metric[N])/N
prom_avg_over() {
  local metric=$1; local seconds=$2
  if [[ -z "$seconds" || "$seconds" -le 0 ]]; then echo 0; return; fi
  local q="sum(increase(${metric}[${seconds}s]))/${seconds}"
  prom_val "$q"
}

say "Resetting and starting a clean pipeline for this demo..."
pkill -f bin/opb >/dev/null 2>&1 || true
pkill -f bin/opa >/dev/null 2>&1 || true
sleep 1

say "Resetting Kafka topics..."
PREFIX=p1 bash scripts/run_infra.sh

WINDOW_DEMO=${WINDOW_DEMO:-120}
say "Starting pipeline (OpA+OpB1) with PebbleDB and ${WINDOW_DEMO}s window..."
export OPB_PEERS="$OPB1_HTTP,$OPB2_HTTP"
GROUP_ID="$GROUP_ID" INSTANCE_ID=B1 STATE_BACKEND=pebble WINDOW_SIZE=${WINDOW_DEMO} bash scripts/start_pipeline.sh
# Ensure demo uses the same window size for ws computation
export EOS_WINDOW_SIZE=${WINDOW_DEMO}

say "Checking health of OpA/OpB..."
if http_ok "$OPA_HTTP/healthz"; then echo "OpA OK"; else echo "OpA NOT READY ($OPA_HTTP/healthz)"; fi
wait_opb_ready() {
  if http_ok "$OPB1_HTTP/healthz"; then
    echo "OpB1 OK"
    return
  fi
  echo "OpB1 NOT READY ($OPB1_HTTP/healthz)"
  say "Waiting for OpB1 to become healthy (max 60s)..."
  for i in {1..60}; do
    sleep 1
    if http_ok "$OPB1_HTTP/healthz"; then
      echo "OpB1 OK after $i s"
      return
    fi
  done
  say "WARN: OpB1 /healthz still 503 after 60s; continuing anyway"
}

wait_opb_ready

########################################
# 1) EOS DEMO (robust)
########################################
# Use isolated key to avoid interference
STORE=${STORE:-EOS-TEST-D-}
PROD=${PROD:-pEOS}
WS=${WS:-$(ws_now)}
N_NEW=${N_NEW:-200}
N_DUP=${N_DUP:-200}
START_BASE=${START_BASE:-$(( ( $(date +%s ) % 1000000 ) + (RANDOM % 1000) ))}
NEW_START=${NEW_START:-$START_BASE}
DUP_START=${DUP_START:-$NEW_START}
SEED_START=${SEED_START:-$(( NEW_START + N_NEW ))}

say "1) EOS demo on exact key: storeId=$STORE productId=$PROD ws=$WS"
EXACT_URL_API="$OPB1_HTTP/api/zone-details?id=$STORE&productId=$PROD&ws=$WS"
EXACT_URL_VIEW="$OPB1_HTTP/viz/zone-data?id=$STORE&productId=$PROD&ws=$WS"
STORE_URL_VIEW="$OPB1_HTTP/viz/zone-data?id=$STORE"

say "OPEN THIS URL in your browser (Store view):"
echo "$STORE_URL_VIEW"
# clickable hyperlink (supported in many terminals)
printf '\e]8;;%s\a%s\e]8;;\a\n' "$STORE_URL_VIEW" "$STORE_URL_VIEW" 2>/dev/null || true
# copy to clipboard on macOS
if command -v pbcopy >/dev/null 2>&1; then printf "%s" "$STORE_URL_VIEW" | pbcopy; echo "(Copied store URL to clipboard)"; fi

echo "Exact page: $EXACT_URL_VIEW"
ask_continue "Ready to inject NEW events (sumQty should increase)?" || { echo Aborted; exit 1; }

# Baseline exact
BASE=$(get_sumqty_exact "$EXACT_URL_API")
echo "Exact baseline sumQty=$BASE (API=$EXACT_URL_API)"

say "Inject NEW $N_NEW events"
NEW_PAYLOAD=$(printf '{"storeId":"%s","productId":"%s","ws":%d,"mode":"new","n":%d,"start":%d}' "$STORE" "$PROD" "$WS" "$N_NEW" "$NEW_START")
curl -s -X POST -H 'Content-Type: application/json' --data-raw "$NEW_PAYLOAD" "$OPB1_HTTP/api/inject-test-data" | cat

# Phase A: wait for exact 200 (key appears)
A_DEADLINE=$(( $(date +%s) + 20 ))
while true; do
  CODE=$(http_code "$EXACT_URL_API")
  printf ".. waiting exact 200, got %s (GET %s)\r" "$CODE" "$EXACT_URL_API"
  if [[ "$CODE" == "200" ]]; then break; fi
  if (( $(date +%s) > A_DEADLINE )); then echo; echo "Seeding 1 event to force key creation"; \
    SEED_START=$(( SEED_START + 1 )); \
    curl -s -X POST -H 'Content-Type: application/json' --data-raw \
      $(printf '{"storeId":"%s","productId":"%s","ws":%d,"mode":"new","n":1,"start":%d}' "$STORE" "$PROD" "$WS" "$SEED_START") \
      "$OPB1_HTTP/api/inject-test-data" >/dev/null || true; \
    A_DEADLINE=$(( $(date +%s) + 20 )); \
  fi
  sleep 1
done

# Phase B: Poll until sumQty increases by at least 1 (more tolerant), up to 120s
TARGET=$(( BASE + 1 ))
B_DEADLINE=$(( $(date +%s) + 120 ))
while true; do
  CUR=$(get_sumqty_exact "$EXACT_URL_API")
  printf ".. exact sumQty=%d / target>=%d (GET %s)\r" "$CUR" "$TARGET" "$EXACT_URL_API"
  if (( CUR >= TARGET )); then break; fi
  if (( $(date +%s) > B_DEADLINE )); then echo; echo "Timeout waiting NEW to settle"; break; fi
  sleep 1
done
# small settle
sleep 1; echo

say "Inject DUPLICATE $N_DUP events (sumQty should NOT change)"
EXPECTED=$(get_sumqty_exact "$EXACT_URL_API")
SKIP_BEFORE=$(metric_val "$OPB1_HTTP" opb_events_skipped_dedup_total || echo 0)
DUP_PAYLOAD=$(printf '{"storeId":"%s","productId":"%s","ws":%d,"mode":"duplicate","n":%d,"start":%d}' "$STORE" "$PROD" "$WS" "$N_DUP" "$DUP_START")
curl -s -X POST -H 'Content-Type: application/json' --data-raw "$DUP_PAYLOAD" "$OPB1_HTTP/api/inject-test-data" | cat

# Poll 10s to ensure unchanged
C_DEADLINE=$(( $(date +%s) + 10 ))
UNCHANGE_OK=1
while true; do
  CUR=$(get_sumqty_exact "$EXACT_URL_API")
  printf ".. after DUP exact sumQty=%d (expected=%d) (GET %s)\r" "$CUR" "$EXPECTED" "$EXACT_URL_API"
  if (( $(date +%s) > C_DEADLINE )); then break; fi
  if (( CUR != EXPECTED )); then UNCHANGE_OK=0; fi
  sleep 1
done
echo
SKIP_AFTER=$(metric_val "$OPB1_HTTP" opb_events_skipped_dedup_total || echo 0)

if (( UNCHANGE_OK == 1 )); then echo "[OK] exact sumQty unchanged after DUP"; else echo "[WARN] exact sumQty changed after DUP"; fi
if [[ -n "$SKIP_BEFORE" && -n "$SKIP_AFTER" ]]; then echo "skipped_dedup delta=$(( ${SKIP_AFTER/.*} - ${SKIP_BEFORE/.*} ))"; fi

echo "Exact page: $EXACT_URL_VIEW"
echo "Store page: $STORE_URL_VIEW"

if [[ "$DEMO_ONLY" == "EOS" ]]; then echo "DEMO_ONLY=EOS -> exit"; exit 0; fi

########################################
# 2) SCALE-OUT DEMO
########################################
say "2) Scale-out: start OpB2 (instance $INSTANCE2_ID) to join group: $GROUP_ID"
if ! http_ok "$OPB2_HTTP/healthz"; then
  mkdir -p "$STATE2_DIR"
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
  sleep 2
fi
if http_ok "$OPB2_HTTP/healthz"; then echo "OpB2 OK at $OPB2_HTTP"; else echo "OpB2 NOT READY ($OPB2_HTTP/healthz)"; fi

say "Pump a bit more random data to spread work..."
STORES_LIST=${STORES_LIST:-"A-,B-,C-,D-,E-,F-,G-,H-,I-,J-,K-,L-,M-,N-,O-,P-"}
N_SPREAD=${N_SPREAD:-5000}
RANDOM_DELAY_MS_MIN=0 RANDOM_DELAY_MS_MAX=80 BATCH=500 STORES="$STORES_LIST" N="$N_SPREAD" bash scripts/pump_random.sh || true

say "Open store page and verify instances include B1 and $INSTANCE2_ID"
URL_SCALE="$OPB1_HTTP/viz/zone-data?id=$STORE"
say "OPEN THIS URL to verify instances (Scale-out view):"
echo "$URL_SCALE"
printf '\e]8;;%s\a%s\e]8;;\a\n' "$URL_SCALE" "$URL_SCALE" 2>/dev/null || true
if command -v pbcopy >/dev/null 2>&1; then printf "%s" "$URL_SCALE" | pbcopy; echo "(Copied scale-out URL to clipboard)"; fi
ask_continue "Did you see instances include both B1 and $INSTANCE2_ID?" || { echo Aborted; exit 1; }

########################################
# 3) THROUGHPUT / LATENCY DEMO
########################################
THR_N=${THR_N:-10000}
say "3) Throughput demo: measure changelog appended delta while pumping $THR_N events"
# Prometheus throughput (events/s) instant over 30s window
TP_BEFORE=$(prom_val 'sum(rate(opb_events_applied_total[30s]))')
echo "prom.rate(opb_events_applied_total[30s]) before: ${TP_BEFORE} e/s"
# Also capture raw counter delta for reference
BEFORE=$(metric_val "$OPB1_HTTP" opb_changelog_appended_total || echo 0)
echo "opb_changelog_appended_total before: ${BEFORE:-0}"
# Start pump (no Prom prints)
N="$THR_N" BATCH=1000 RANDOM_DELAY_MS_MIN=0 RANDOM_DELAY_MS_MAX=20 bash scripts/pump_random.sh || true
# Raw counter delta
AFTER=$(metric_val "$OPB1_HTTP" opb_changelog_appended_total || echo 0)
echo "opb_changelog_appended_total after: ${AFTER:-0}"
if [[ -n "${BEFORE}" && -n "${AFTER}" ]]; then
  echo "delta=$(( ${AFTER/.*} - ${BEFORE/.*} ))"
fi
# Wait until lag drains to ~0 (max 60s)
DEADLINE=$(( $(date +%s) + 60 ))
while true; do
  LAG_NOW=$(prom_val 'sum(opb_partition_lag)')
  printf ".. waiting lag -> 0 (now=%s)\r" "$LAG_NOW"
  # treat values <1 as 0 (strings possible)
  LAG_INT=${LAG_NOW%.*}
  if [[ -z "$LAG_INT" ]]; then LAG_INT=0; fi
  if (( LAG_INT <= 0 )); then echo; break; fi
  if (( $(date +%s) > DEADLINE )); then echo; echo "WARN: lag not zero after 60s"; break; fi
  sleep 1
done
# One more rate after drain
TP_AFTER=$(prom_val 'sum(rate(opb_events_applied_total[30s]))')
echo "prom.rate(opb_events_applied_total[30s]) after drain: ${TP_AFTER} e/s"

say "Latency demo: inject one exact event and measure time to appear on exact endpoint"
WS_LAT=${WS_LAT:-$(ws_now)}
T0=$(date +%s)
# Inject single event
curl -s -X POST -H 'Content-Type: application/json' --data-raw \
  $(printf '{"storeId":"%s","productId":"%s","ws":%d,"mode":"new","n":1}' "$STORE" "$PROD" "$WS_LAT") \
  "$OPB1_HTTP/api/inject-test-data" >/dev/null || true
# Poll exact up to 30s for sum change
TARGET_URL="$OPB1_HTTP/api/zone-details?id=$STORE&productId=$PROD&ws=$WS_LAT"
BASE_SUM=$(get_sumqty_exact "$TARGET_URL")
for i in {1..30}; do
  CUR=$(get_sumqty_exact "$TARGET_URL")
  if (( CUR > BASE_SUM )); then break; fi
  sleep 1
done
T1=$(date +%s)
echo "Exact latency ~= $((T1-T0)) s (from inject to visible on exact endpoint)"

say "Demo suite completed."
