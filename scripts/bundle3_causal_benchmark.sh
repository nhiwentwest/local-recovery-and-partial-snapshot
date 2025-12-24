#!/usr/bin/env bash
set -euo pipefail
# Bundle 3 — Causal Inflight + Freeze + Epoch Fencing (Benchmark-only)
# - No UI pages, no baseline scenario
# - 3 OpB instances (B1/B2/B3), >=12 partitions
# - Heavy inflight backlog; Freeze to encourage tail skip
# - Emits CSV summary per run

# -------- Config (env overridable) --------
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
BIN_OPB=${BIN_OPB:-./bin/opb}
BIN_KADMIN=${BIN_KADMIN:-./bin/kadmin}
BIN_GEN=${BIN_GENORDERS:-./bin/genorders}

GROUP_ID=${GROUP_ID:-opb-b3c-$(date +%s)}
ENRICHED_TOPIC=${ENRICHED_TOPIC:-p1.orders.enriched}
TOPIC_CL=${TOPIC_CL:-p1.opb-changelog}
TOPIC_SNAP=${TOPIC_SNAP:-p1.opb-snapshots}

ENRICHED_PARTITIONS=${ENRICHED_PARTITIONS:-12}
CHANGELOG_PARTITIONS=${CHANGELOG_PARTITIONS:-4}
SNAPSHOTS_PARTITIONS=${SNAPSHOTS_PARTITIONS:-2}

HTTP1=${HTTP1:-:8089}
HTTP2=${HTTP2:-:8090}
HTTP3=${HTTP3:-:8091}
OPB1=${OPB1:-http://127.0.0.1:8089}
OPB2=${OPB2:-http://127.0.0.1:8090}
OPB3=${OPB3:-http://127.0.0.1:8091}

STATE_DIR=${STATE_DIR:-./data/b3c}
SNAPSHOT_DIR=${SNAPSHOT_DIR:-./snapshots-b3c}
CHANGELOG_DIR=${CHANGELOG_DIR:-./changelog-b3c}
LOG_DIR=${LOG_DIR:-./logs}
RESULTS_CSV=${RESULTS_CSV:-./results/bundle3_causal_benchmark.csv}
mkdir -p "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR" "$LOG_DIR" "$(dirname "$RESULTS_CSV")" 2>/dev/null || true
# Normalize directories to absolute paths to avoid CWD mismatches across background processes
STATE_DIR="$(cd "$STATE_DIR" && pwd)"
SNAPSHOT_DIR="$(cd "$SNAPSHOT_DIR" && pwd)"
CHANGELOG_DIR="$(cd "$CHANGELOG_DIR" && pwd)"
LOG_DIR="$(cd "$LOG_DIR" && pwd)"
RESULTS_CSV_DIR="$(cd "$(dirname "$RESULTS_CSV")" && pwd)"
RESULTS_CSV="$RESULTS_CSV_DIR/$(basename "$RESULTS_CSV")"

# Workload knobs
WINDOW_SIZE=${WINDOW_SIZE:-3600}
SEED_STORES=${SEED_STORES:-600}
SEED_PRODUCTS=${SEED_PRODUCTS:-1500}
SEED_N_PER_KEY=${SEED_N_PER_KEY:-1}
SEED_LINGER_MS=${SEED_LINGER_MS:-10}

STORE=${STORE:-B3C}
PROD=${PROD:-p1}
DELTA_BASE_EVENTS=${DELTA_BASE_EVENTS:-100000}  # inflight size for main key (default 100k)
POST_CUT_EVENTS=${POST_CUT_EVENTS:-0}          # keep 0 for freeze-oriented benchmark
REPEATS=${REPEATS:-1}

# Delta mode: p2=external SST delta (default), p3=incremental shipping
DELTA_MODE=${DELTA_MODE:-p2}
# If ENABLE_PEBBLE_PHASE3 is not explicitly set, derive from DELTA_MODE
if [[ -z "${ENABLE_PEBBLE_PHASE3:-}" ]]; then
  if [[ "$DELTA_MODE" == "p3" ]]; then ENABLE_PEBBLE_PHASE3=1; else ENABLE_PEBBLE_PHASE3=0; fi
fi
PEBBLE_FLAG=""; [[ "$ENABLE_PEBBLE_PHASE3" == "1" ]] && PEBBLE_FLAG="--enable-pebble-phase3"

say(){ printf "\n\e[1;36m[B3C]\e[0m %s\n" "$*"; }
http_ok(){ curl -sf "$1" >/dev/null 2>&1; }
wait_ready(){ local url=$1; local n=${2:-180}; for((i=1;i<=n;i++)); do http_ok "$url" && return 0; sleep 1; done; return 1; }

require_bins(){
  if [[ ! -x "$BIN_KADMIN" ]]; then say "Building kadmin"; go build -o "$BIN_KADMIN" ./cmd/kadmin; fi
  if [[ ! -x "$BIN_OPB" ]]; then say "Building opb"; go build -o "$BIN_OPB" ./cmd/opb; fi
  if [[ ! -x "$BIN_GEN" ]]; then say "Building genorders"; go build -o "$BIN_GEN" ./cmd/genorders; fi
  if ! command -v jq >/dev/null 2>&1; then echo "ERROR: jq required" >&2; exit 1; fi
}

kadmin(){ "$BIN_KADMIN" -bootstrap "$BOOTSTRAP" "$@"; }

ensure_topics(){
  say "Reset topics"
  kadmin -cmd delete -topic "$ENRICHED_TOPIC" || true
  kadmin -cmd delete -topic "$TOPIC_CL" || true
  kadmin -cmd delete -topic "$TOPIC_SNAP" || true
  kadmin -cmd create -topic "$ENRICHED_TOPIC" -partitions "$ENRICHED_PARTITIONS" -rf 1 || true
  kadmin -cmd create -topic "$TOPIC_CL" -partitions "$CHANGELOG_PARTITIONS" -rf 1 -config cleanup.policy=delete || true
  kadmin -cmd create -topic "$TOPIC_SNAP" -partitions "$SNAPSHOTS_PARTITIONS" -rf 1 -config cleanup.policy=compact || true
}

clean_state(){ rm -rf "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR" "$STATE_DIR".b2 "$CHANGELOG_DIR".b2 "$STATE_DIR".b3 "$CHANGELOG_DIR".b3; mkdir -p "$STATE_DIR" "$SNAPSHOT_DIR" "$CHANGELOG_DIR"; }

stop_all(){ pkill -f "\bopb\b" >/dev/null 2>&1 || true; }

start_b1(){
  say "Start B1"
  nohup "$BIN_OPB" \
    --state-backend pebble --state-dir "$STATE_DIR" --snapshot-dir "$SNAPSHOT_DIR" \
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
    --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
    --changelog-dir "$CHANGELOG_DIR" \
    --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
    --manifest-sink both --manifest-source file --changelog-sink both --changelog-source kafka \
    --snapshot-interval 0 --window-size "$WINDOW_SIZE" \
    $PEBBLE_FLAG --peers "$OPB2,$OPB3" --http "$HTTP1" --instance-id B1 \
    > "$LOG_DIR/b3c_b1.log" 2>&1 &
  wait_ready "$OPB1/healthz" 180 || { say "B1 failed"; tail -n 120 "$LOG_DIR/b3c_b1.log" || true; exit 1; }
  for i in 1 2 3; do http_ok "$OPB1/healthz" && sleep 1 || { say "B1 unhealthy after ready"; tail -n 120 "$LOG_DIR/b3c_b1.log" || true; exit 1; }; done
}

start_peer(){ # $1=name B2/B3, $2=stateDir, $3=http, $4=url
  local name=$1 sdir=$2 http=$3 url=$4
  say "Start $name"
  nohup "$BIN_OPB" \
    --state-backend pebble --state-dir "$sdir" --snapshot-dir "$SNAPSHOT_DIR" \
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
    --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
    --changelog-dir "$sdir".cl \
    --rebalance-import-state=true --peers "$OPB1,$OPB2" \
    --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
    --manifest-sink both --manifest-source kafka --changelog-sink both --changelog-source kafka \
    --snapshot-interval 0 --window-size "$WINDOW_SIZE" \
    $PEBBLE_FLAG --http "$http" --instance-id ${name} \
    > "$LOG_DIR/b3c_${name}.log" 2>&1 &
  wait_ready "$url/healthz" 180 || { say "$name failed"; tail -n 120 "$LOG_DIR/b3c_${name}.log" || true; exit 1; }
  for i in 1 2 3; do http_ok "$url/healthz" && sleep 1 || { say "$name unhealthy after ready"; tail -n 120 "$LOG_DIR/b3c_${name}.log" || true; exit 1; }; done
}

seed_base(){
  say "Seeding base events"
  "$BIN_GEN" --mode kafka \
    --bootstrap "$BOOTSTRAP" --topic "$ENRICHED_TOPIC" \
    --stores "$SEED_STORES" --products "$SEED_PRODUCTS" \
    --n-per-key "$SEED_N_PER_KEY" --window-size "$WINDOW_SIZE" --linger-ms "$SEED_LINGER_MS" \
    > "$LOG_DIR/b3c_seed.log" 2>&1 || true
}

get_lag(){ local data; data=$(curl -s "$OPB1/status" || echo "{}"); printf '%s' "$data" | sed -n 's/.*"lagTotal"[[:space:]]*:[[:space:]]*\([0-9.][0-9.]*\).*/\1/p' | head -n1; }
wait_lag0(){ local t=${DRAIN_TIMEOUT:-600}; for ((i=1;i<=t;i++)); do local l=$(get_lag); [[ -z "$l" ]] && l=0; awk "BEGIN{exit !($l<=1)}" && return 0; sleep 1; done; return 1; }

cut_full(){ say "Cut full snapshot"; curl -fsS -X POST "$OPB1/admin/snapshot-cut?type=full" >/dev/null; }
cut_delta(){ say "Cut delta snapshot"; curl -fsS -X POST "$OPB1/admin/snapshot-cut?type=delta" >/dev/null; }

wait_manifest_offsets(){ local t=${1:-240}; say "Wait manifest with offsets (up to ${t}s)"; for((i=1;i<=t;i++)); do if [[ -f "$SNAPSHOT_DIR/manifest.latest.json" ]]; then local n=$(jq -r '.changelog.offsets | length' "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || echo 0); if [[ "$n" =~ ^[0-9]+$ ]] && (( n>0 )); then return 0; fi; fi; sleep 1; done; return 1; }
wait_manifest_inflight(){ local t=${1:-120}; for((i=1;i<=t;i++)); do if [[ -f "$SNAPSHOT_DIR/manifest.latest.json" ]]; then local f=$(jq -r '.inflightFile // ""' "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || echo ""); [[ -n "$f" && "$f" != "null" ]] && return 0; fi; sleep 1; done; return 1; }

wait_manifest_published(){ local dir=${1:-$SNAPSHOT_DIR}; local timeout=${2:-60}; for((i=1;i<=timeout;i++)); do if [[ -f "$dir/manifest.latest.json" ]]; then local sid=$(jq -r '.snapshotId // ""' "$dir/manifest.latest.json" 2>/dev/null || echo ""); if [[ -n "$sid" && "$sid" != "null" ]]; then echo "manifest ready: snapshotId=$sid"; return 0; fi; fi; sleep 1; done; echo "WARN: manifest.latest.json not observed in $dir within ${timeout}s"; return 1; }

# Wait until causal barrier finalized (markersSeen==markersTotal>0)
wait_causal_finalized(){ local base=${1:-$OPB1}; local timeout=${2:-600}; echo "Waiting causal finalize up to ${timeout}s..."; for((i=1;i<=timeout;i++)); do local j id seen tot; j=$(curl -s "$base/status" || true); id=$(jq -r '.causalCutId // ""' <<<"$j"); seen=$(jq -r '.causalMarkersSeen // 0' <<<"$j"); tot=$(jq -r '.causalMarkersTotal // 0' <<<"$j"); if [[ -n "$id" && "$tot" =~ ^[0-9]+$ && "$seen" =~ ^[0-9]+$ && $tot -gt 0 && $seen -eq $tot ]]; then echo "causal finalized: id=$id markers=$seen/$tot"; return 0; fi; sleep 1; done; echo "WARN: causal finalize timeout"; return 1; }

freeze_mark(){ # mark replayRequired=false on latest and archived manifest
  say "Freeze: mark replayRequired=false on manifest(s)"
  local mani="$SNAPSHOT_DIR/manifest.latest.json"
  if [[ -f "$mani" ]]; then
    local sid
    sid=$(jq -r '.snapshotId // ""' "$mani" 2>/dev/null || echo "")
    tmp=$(mktemp) || true; jq '.replayRequired=false' "$mani" > "$tmp" 2>/dev/null || true
    [[ -s "$tmp" ]] && mv "$tmp" "$mani"
    if [[ -n "$sid" && -f "$SNAPSHOT_DIR/$sid/manifest.json" ]]; then
      tmp2=$(mktemp) || true; jq '.replayRequired=false' "$SNAPSHOT_DIR/$sid/manifest.json" > "$tmp2" 2>/dev/null || true
      [[ -s "$tmp2" ]] && mv "$tmp2" "$SNAPSHOT_DIR/$sid/manifest.json"
    fi
  else
    say "WARN: manifest.latest.json not found; skip freeze mark"
  fi
}

parse_restore_csv(){ # log -> csv fields (fallback to metrics file if phases missing)
  local logf=$1
  local metricsf="$STATE_DIR.b3/restore-metrics.json"
  local ttr=$(grep -E "restore completed: .*elapsedMs=[0-9]+" "$logf" | tail -n1 | sed -E 's/.*elapsedMs=([0-9]+).*/\1/' || echo "")
  local phases=$(grep -F "restore phases:" "$logf" | tail -n1 | sed -E 's/.*restore phases: //')
  local mani=$(jq -r '.timings.manifestMs // ""' <<<"$phases" 2>/dev/null || echo "")
  local snap=$(jq -r '.timings.snapshotTotalMs // ""' <<<"$phases" 2>/dev/null || echo "")
  local inflms=$(jq -r '.timings.inflightMs // ""' <<<"$phases" 2>/dev/null || echo "")
  local clog=$(jq -r '.timings.changelogMs // ""' <<<"$phases" 2>/dev/null || echo "")
  local total=$(jq -r '.timings.totalMs // ""' <<<"$phases" 2>/dev/null || echo "")
  local applied=$(grep -E "restore completed: applied=[0-9]+" "$logf" | tail -n1 | sed -E 's/.*applied=([0-9]+).*/\1/' || echo "")
  local infl=$(grep -E "inflight replay applied" "$logf" | tail -n1 | sed -E 's/.*events=([0-9]+).*/\1/' || echo "0")
  if [[ ( -z "$mani" || -z "$total" ) && -f "$metricsf" ]]; then
    mani=$(jq -r '.phases.manifestMs // ""' "$metricsf" 2>/dev/null || echo "")
    snap=$(jq -r '.phases.snapshotTotalMs // ""' "$metricsf" 2>/dev/null || echo "")
    inflms=$(jq -r '.phases.inflightMs // ""' "$metricsf" 2>/dev/null || echo "")
    clog=$(jq -r '.phases.changelogMs // ""' "$metricsf" 2>/dev/null || echo "")
    total=$(jq -r '.phases.totalMs // ""' "$metricsf" 2>/dev/null || echo "")
    if [[ -z "$ttr" ]]; then ttr=$(jq -r '.ttrMs // ""' "$metricsf" 2>/dev/null || echo ""); fi
  fi
  echo "$mani,$snap,$inflms,$clog,$total,$ttr,$applied,$infl"
}

run_restore_once(){ # $1 flags, $2 http, $3 log (foreground; blocks until restore-only exits)
  local flags=$1 http=$2 logf=$3
  "$BIN_OPB" \
    --state-backend pebble --state-dir "$STATE_DIR.b3" --snapshot-dir "$SNAPSHOT_DIR" \
    --kafka-bootstrap "$BOOTSTRAP" --group-id "$GROUP_ID" \
    --input-source kafka --topic-enriched "$ENRICHED_TOPIC" \
    --changelog-dir "$CHANGELOG_DIR.b3" \
    --topic-snapshots "$TOPIC_SNAP" --topic-changelog "$TOPIC_CL" \
    --manifest-sink both --manifest-source file --changelog-sink both --changelog-source kafka \
    --snapshot-interval 0 \
    --window-size "$WINDOW_SIZE" --restore-on-start --restore-only $PEBBLE_FLAG $flags \
    --http "$http" --instance-id B3 > "$logf" 2>&1 || true
}

kill_b3(){
  say "Stopping B3 instance on ${HTTP3}..."
  local port
  port=$(echo "$HTTP3" | sed 's/://')
  local pid
  pid=$(lsof -ti tcp:"$port" 2>/dev/null || true)
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    # Wait for process to exit and release lock
    for ((i=1;i<=10;i++)); do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        say "✓ B3 process (PID $pid) exited."
        pid=""
        break
      fi
      sleep 0.5
    done
    if [[ -n "$pid" ]]; then
      say "WARN: B3 process (PID $pid) did not exit gracefully. Forcing kill."
      kill -9 "$pid" 2>/dev/null || true
    fi
  else
    say "B3 process not found on port $port."
  fi
  # Final check for lock file
  local lock_file="$STATE_DIR.b3/LOCK"
  if [[ -f "$lock_file" ]]; then
    say "WARN: Lock file $lock_file still exists. Waiting a bit more..."
    sleep 2
    if [[ -f "$lock_file" ]]; then
      say "ERROR: Lock file still exists. Manual intervention may be needed."
      # As a last resort for benchmark script, remove it.
      rm -f "$lock_file"
      say "Force-removed lock file."
    fi
  fi
}

header(){ echo "parts,inflightEvents,postCutEvents,manifestMs,snapshotMs,inflightMs,changelogMs,totalMs,ttrMs,applied,causalReplay"; }

run_once(){
  say "BEGIN causal_freeze benchmark"
  clean_state; ensure_topics
  start_b1; start_peer B2 "$STATE_DIR.b2" "$HTTP2" "$OPB2"; start_peer B3 "$STATE_DIR.b3" "$HTTP3" "$OPB3"
  seed_base; # no strict wait here to reduce overhead
  cut_full; wait_manifest_offsets 300 || say "WARN: manifest offsets timeout"
  # Build inflight backlog
  say "Pause ingest, inject inflight backlog n=$DELTA_BASE_EVENTS"
  curl -fsS -X POST "$OPB1/admin/ingest/pause" >/dev/null; sleep 1
  WS=$(( $(date +%s)/WINDOW_SIZE*WINDOW_SIZE ))
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d "[{\"storeId\":\"$STORE\",\"productId\":\"$PROD\",\"ws\":$WS,\"mode\":\"new\",\"n\":$DELTA_BASE_EVENTS,\"start\":1000}]" \
    "$OPB1/api/inject-test-data" >/dev/null
  sleep 2
  cut_delta; sleep 1; curl -fsS -X POST "$OPB1/admin/ingest/resume" >/dev/null
  # Wait for barrier to finalize across all partitions and manifest to be published (file-based restore relies on it)
  # Removed blocking causal finalize wait; rely on manifest publish + lag zero
  wait_manifest_published "$SNAPSHOT_DIR" 600 || say "WARN: manifest not ready after resume"
  wait_manifest_inflight 300 || say "WARN: inflight not observed"

  # Drain remaining lag while RUNNING to ensure state is caught up before freeze
  say "Draining remaining lag before freeze..."
  wait_lag0 || say "WARN: lag not fully drained before pause/freeze"

  say "Lag is zero. Pausing to seal epoch."
  curl -fsS -X POST "$OPB1/admin/ingest/pause" >/dev/null
  sleep 1 # allow pause to take effect

  # Ensure manifest.latest.json is present before freeze & restore (file-based restore relies on it)
  wait_manifest_published "$SNAPSHOT_DIR" 600 || say "WARN: manifest not ready before freeze"
  # Mark the manifest for freeze
  freeze_mark

  # Ensure manifest is present right before restore-only as well
  wait_manifest_published "$SNAPSHOT_DIR" 180 || say "WARN: manifest not ready before restore"
  # Restore with the frozen manifest
  kill_b3; run_restore_once "--manifest-source file --restore-trust-manifest --inflight-workers ${INFLIGHT_WORKERS:-0}" :8092 "$LOG_DIR/b3c_restore.log"

  # Wait for restore to complete by polling the log file to avoid race condition
  say "Waiting for restore-only to complete..."
  local restore_done=0
  for ((i=1;i<=120;i++)); do
    if grep -q "restore-only: exiting after successful restore" "$LOG_DIR/b3c_restore.log" 2>/dev/null; then
      say "✓ Restore completed."
      restore_done=1
      break
    fi
    sleep 1
  done
  if [[ "$restore_done" -eq 0 ]]; then
    say "WARN: Restore did not complete within timeout. Parsing partial logs."
  fi

  local csv; csv=$(parse_restore_csv "$LOG_DIR/b3c_restore.log")
  IFS=',' read -r mani snap clog total ttr applied infl <<<"$csv"
  local inflightCount=0
  if [[ -f "$SNAPSHOT_DIR/manifest.latest.json" ]]; then inflightCount=$(jq -r '.inflightEvents // 0' "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || echo 0); fi
  printf "%d,%d,%d,%s,%s,%s,%s,%s,%s,%s\n" \
    "$ENRICHED_PARTITIONS" "$inflightCount" "$POST_CUT_EVENTS" \
    "$mani" "$snap" "$clog" "$total" "$ttr" "$applied" "$infl"
  stop_all
}

main(){
  require_bins
  header | tee "$RESULTS_CSV" >/dev/null || true
  for ((r=1;r<=REPEATS;r++)); do
    run_once | tee -a "$RESULTS_CSV"
  done
  say "Done. Results at $RESULTS_CSV"
}

main "$@"

