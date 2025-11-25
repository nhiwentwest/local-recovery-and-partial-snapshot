#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPB_BIN="${OPB_BIN:-$ROOT_DIR/bin/opb}"
BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
INSTANCE_ID="${INSTANCE_ID:-B1}"
GROUP_ID="${GROUP_ID:-opb-e2e}"
HTTP_ADDR="${HTTP_ADDR:-:8089}"
RESTORE_HTTP_ADDR="${RESTORE_HTTP_ADDR:-$HTTP_ADDR}"
STATE_BACKEND="${STATE_BACKEND:-pebble}"
STATE_DIR="${STATE_DIR:-$ROOT_DIR/data/opb}"
RESTORE_STATE_DIR="${RESTORE_STATE_DIR:-$ROOT_DIR/data/opb-restore-only}"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-$ROOT_DIR/snapshots}"
SNAPSHOT_FORMAT="${SNAPSHOT_FORMAT:-json}"
SNAPSHOT_SHARDS="${SNAPSHOT_SHARDS:-4}"
PUMP_AFTER_CUT="${PUMP_AFTER_CUT:-0}"
PUMP_APPLY_SEC="${PUMP_APPLY_SEC:-5}"
INJECT_STORE="${INJECT_STORE:-TTR-STORE-}"
INJECT_PRODUCT="${INJECT_PRODUCT:-p1}"
MULTI_STORES="${MULTI_STORES:-4}"
PRUNE_BEFORE_SNAPSHOT="${PRUNE_BEFORE_SNAPSHOT:-0}"
PRUNE_WS_BEFORE="${PRUNE_WS_BEFORE:-0}"
PRUNE_LIMIT="${PRUNE_LIMIT:-0}"
PRUNE_STORE_ID="${PRUNE_STORE_ID:-}"
PRUNE_PRODUCT_ID="${PRUNE_PRODUCT_ID:-}"
PRUNE_DRY_RUN="${PRUNE_DRY_RUN:-0}"
TOPIC_PREFIX="${TOPIC_PREFIX:-p1}"
TOPIC_ENRICHED="${TOPIC_ENRICHED:-$TOPIC_PREFIX.orders.enriched}"
TOPIC_OUTPUT="${TOPIC_OUTPUT:-$TOPIC_PREFIX.orders.output}"
TOPIC_CHANGELOG="${TOPIC_CHANGELOG:-$TOPIC_PREFIX.opb-changelog}"
TOPIC_SNAPSHOTS="${TOPIC_SNAPSHOTS:-$TOPIC_PREFIX.opb-snapshots}"
SNAPSHOT_WAIT="${SNAPSHOT_WAIT:-5}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/opb_restore_only.log}"
SKIP_SNAPSHOT_CUT="${SKIP_SNAPSHOT_CUT:-0}"
STRIP_OFFSETS="${STRIP_OFFSETS:-0}"

mkdir -p "$LOG_DIR"

if [[ ! -x "$OPB_BIN" ]]; then
  echo "[ttr] missing opb binary at $OPB_BIN" >&2
  exit 1
fi

http_url="$HTTP_ADDR"
if [[ "$http_url" == http://* || "$http_url" == https://* ]]; then
  true
elif [[ "$http_url" == :* ]]; then
  http_url="http://127.0.0.1$http_url"
else
  http_url="http://$http_url"
fi



if [[ "$PRUNE_BEFORE_SNAPSHOT" == "1" && "$PRUNE_WS_BEFORE" -gt 0 ]]; then
  echo "[ttr] pruning state before snapshot (ws<$PRUNE_WS_BEFORE limit=$PRUNE_LIMIT store=$PRUNE_STORE_ID product=$PRUNE_PRODUCT_ID)"
  dry="false"
  if [[ "$PRUNE_DRY_RUN" == "1" ]]; then
    dry="true"
  fi
  payload=$(cat <<JSON
{
  "storeId": "$PRUNE_STORE_ID",
  "productId": "$PRUNE_PRODUCT_ID",
  "windowStartBefore": $PRUNE_WS_BEFORE,
  "limit": $PRUNE_LIMIT,
  "dryRun": $dry
}
JSON
)
  if ! curl -s -X POST -H "Content-Type: application/json" -d "$payload" "$http_url/admin/prune-state" | jq '.'; then
    echo "[ttr] prune-state request failed" >&2
    exit 1
  fi
fi

if [[ "$SKIP_SNAPSHOT_CUT" == "1" ]]; then
  echo "[ttr] SKIP_SNAPSHOT_CUT=1 -> reusing existing manifest"
else
  CUT_TYPE="${CUT_TYPE:-full}"
  echo "[ttr] triggering snapshot cut ($CUT_TYPE) via $http_url/admin/snapshot-cut?type=$CUT_TYPE"
  if ! curl -s -X POST "$http_url/admin/snapshot-cut?type=$CUT_TYPE" | jq '.'; then
    echo "[ttr] snapshot-cut request failed" >&2
    exit 1
  fi
  echo "[ttr] waiting $SNAPSHOT_WAIT s for snapshot/manifest to settle"
  sleep "$SNAPSHOT_WAIT"

  # In non-blocking mode, wait until manifest contains per-partition offsets (best-effort)
  WAIT_OFFSETS_SEC="${WAIT_OFFSETS_SEC:-30}"
  for ((w=1; w<=WAIT_OFFSETS_SEC; w++)); do
    if [[ -f "$SNAPSHOT_DIR/manifest.latest.json" ]]; then
      HAS_OFFS=$(jq -r '.changelog | if . != null and (.offsets|length) > 0 then "yes" else "no" end' "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || echo "no")
      if [[ "$HAS_OFFS" == "yes" ]]; then
        echo "[ttr] manifest has per-partition offsets (barrier cut ready)"
        break
      fi
    fi
    printf "[ttr] waiting for manifest offsets... %ds/%ds\r" "$w" "$WAIT_OFFSETS_SEC"
    sleep 1
  done
  echo

  # Pin current manifest to avoid periodic publisher overriding after we create backlog
  if [[ -f "$SNAPSHOT_DIR/manifest.latest.json" ]]; then
    cp -f "$SNAPSHOT_DIR/manifest.latest.json" "$SNAPSHOT_DIR/manifest.latest.pinned.json" || true
    echo "[ttr] pinned manifest to $SNAPSHOT_DIR/manifest.latest.pinned.json"
    echo "[ttr] chain manifest (latest):"
    jq '{snapshotId, snapshotType, baseSnapshotId, parentSnapshotId, deltaSequence, snapshotFormat, snapshotShards, snapshotKeys, changelog} | .changelog.offsets |= ( . // [] )' "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || true
  fi

  # Optionally create backlog AFTER the cut to exercise changelog replay
  if [[ "$PUMP_AFTER_CUT" =~ ^[0-9]+$ ]] && [[ "$PUMP_AFTER_CUT" -gt 0 ]]; then
    # Ensure ingest is running (best-effort)
    curl -s -X POST "$http_url/admin/ingest/resume" >/dev/null 2>&1 || true
    NOW=$(date +%s)
    WS=$(( (NOW/60) * 60 ))
    echo "[ttr] injecting $PUMP_AFTER_CUT events across $MULTI_STORES stores"
    PER_STORE=$(( (PUMP_AFTER_CUT + MULTI_STORES - 1) / MULTI_STORES ))
    for i in $(seq 1 "$MULTI_STORES"); do
      STORE_ID="${INJECT_STORE}${i}-"
      START=$(( (i-1) * PER_STORE ))
      INJ_PAYLOAD=$(cat <<JSON
{"storeId":"$STORE_ID","productId":"$INJECT_PRODUCT","ws":$WS,"mode":"new","n":$PER_STORE,"start":$START}
JSON
)
      curl -s -X POST -H 'Content-Type: application/json' -d "$INJ_PAYLOAD" "$http_url/api/inject-test-data" | jq '.' || true
    done
    echo "[ttr] waiting $PUMP_APPLY_SEC s for changelog to be written"
    sleep "$PUMP_APPLY_SEC"
    # Wait for backlog based on Kafka high watermarks vs manifest offsets
    WAIT_BACKLOG_SEC="${WAIT_BACKLOG_SEC:-120}"
    echo "[ttr] waiting for backlog >= $PUMP_AFTER_CUT based on offsets (timeout ${WAIT_BACKLOG_SEC}s)"
    for ((w=1; w<=WAIT_BACKLOG_SEC; w++)); do
      WM_JSON=$("$ROOT_DIR/bin/kadmin" -bootstrap "$BOOTSTRAP" -cmd watermarks -topic "$TOPIC_CHANGELOG" 2>/dev/null || echo '{}')
      MAN_JSON=$(cat "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || echo '{}')

      # Ensure variables are not empty before passing to jq
      WM_JSON=${WM_JSON:-'{}'}
      MAN_JSON=${MAN_JSON:-'{}'}

      DELTA=$(jq -n --argjson wm "$WM_JSON" --argjson man "$MAN_JSON" \
        '($wm.high // []) as $H | ($man.changelog.offsets // []) as $O | reduce range(0; ($H|length)) as $i (0; . + (($H[$i] // 0) - ($O[$i] // 0)))' 2>/dev/null || echo 0)
      printf "[ttr] backlog delta=%s (target=%s)\\r" "$DELTA" "$PUMP_AFTER_CUT"
      if [[ "$DELTA" =~ ^[0-9]+$ ]] && (( DELTA >= PUMP_AFTER_CUT )); then
        echo
        echo "[ttr] backlog threshold reached"
        break
      fi
      sleep 1
    done
  fi
fi

pattern="${OPB_PATTERN:-opb --kafka-bootstrap}"
if pgrep -f "$pattern" >/dev/null 2>&1; then
  echo "[ttr] stopping existing opb instances"
  pkill -f "$pattern" || true
  sleep 2
fi
# Optionally restore pinned manifest; set PREFER_LATEST=1 to ignore pinned
if [[ -f "$SNAPSHOT_DIR/manifest.latest.pinned.json" ]]; then
  if [[ "${PREFER_LATEST:-0}" == "1" ]]; then
    echo "[ttr] PREFER_LATEST=1 -> ignoring pinned manifest, using latest"
  else
  if [[ "$STRIP_OFFSETS" == "1" ]]; then
    echo "[ttr] restoring pinned manifest, stripping per-partition offsets and resetting lastChangelogOffset=0"
    cat "$SNAPSHOT_DIR/manifest.latest.pinned.json" | jq 'del(.changelog) | .lastChangelogOffset=0' > "$SNAPSHOT_DIR/manifest.latest.json" || cp -f "$SNAPSHOT_DIR/manifest.latest.pinned.json" "$SNAPSHOT_DIR/manifest.latest.json"
  else
    echo "[ttr] restoring pinned manifest as-is (preserve per-partition offsets)"
    cp -f "$SNAPSHOT_DIR/manifest.latest.pinned.json" "$SNAPSHOT_DIR/manifest.latest.json" || true
  fi
  echo "[ttr] restored pinned manifest to $SNAPSHOT_DIR/manifest.latest.json"
fi
fi

# Show manifest summary (latest on disk)
echo "[ttr] chain manifest (using):"
jq '{snapshotId, snapshotType, baseSnapshotId, parentSnapshotId, deltaSequence, snapshotFormat, snapshotShards, snapshotKeys, changelog} | .changelog.offsets |= ( . // [] )' "$SNAPSHOT_DIR/manifest.latest.json" 2>/dev/null || true

now_ms() {
python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

cmd=(
  "$OPB_BIN"
  --kafka-bootstrap "$BOOTSTRAP"
  --input-source kafka
  --group-id "$GROUP_ID"
  --state-backend "$STATE_BACKEND"
  --state-dir "$RESTORE_STATE_DIR"
  --window-size 60
  --instance-id "$INSTANCE_ID"
  --http "$RESTORE_HTTP_ADDR"
  --changelog-sink kafka
  --manifest-sink file
  --changelog-source kafka
  --manifest-source file
  --topic-enriched "$TOPIC_ENRICHED"
  --output-topic "$TOPIC_OUTPUT"
  --topic-changelog "$TOPIC_CHANGELOG"
  --topic-snapshots "$TOPIC_SNAPSHOTS"
  --snapshot-dir "$SNAPSHOT_DIR"
  --snapshot-interval 0
  --snapshot-format "$SNAPSHOT_FORMAT"
  --snapshot-shards "$SNAPSHOT_SHARDS"
  --restore-parallelism "${RESTORE_PARALLELISM:-0}"
  --replay-workers "${REPLAY_WORKERS:-0}"
  --restore-on-start
  --restore-only
)

echo "[ttr] running restore-only: ${cmd[*]}"
start_ms="$(now_ms)"
set +e
"${cmd[@]}" &> "$LOG_FILE"
rc=$?
set -e
end_ms="$(now_ms)"
dur_ms=$((end_ms - start_ms))
echo "[ttr] restore-only exited rc=$rc in ${dur_ms}ms (log: $LOG_FILE)"

tail -n 5 "$LOG_FILE"

phase_line="$(grep -m1 'restore phases:' "$LOG_FILE" || true)"
if [[ -n "$phase_line" ]]; then
  phase_json="${phase_line#*restore phases: }"
  echo "[ttr] phase timings: $phase_json"
fi

exit "$rc"

