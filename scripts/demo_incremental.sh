#!/usr/bin/env bash
set -euo pipefail

SNAPSHOT_DIR=${SNAPSHOT_DIR:-./snapshots}
STATE_DIR=${STATE_DIR:-./data/opb}
OPB_BIN=${OPB_BIN:-./opb}
OPB_TOOL_BIN=${OPB_TOOL_BIN:-./opbtool}
LOG_FILE=${LOG_FILE:-./opb.log}

say() { printf '[demo-incremental] %s\n' "$*"; }

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require jq

latest_manifest() {
  local manifest=${SNAPSHOT_DIR}/manifest.latest.json
  if [[ ! -f "$manifest" ]]; then
    echo ""
    return
  fi
  jq -r '.snapshotId // ""' "$manifest"
}

inspect_snapshot() {
  local sid=$1
  if [[ -z "$sid" ]]; then
    say "No snapshot ID provided"
    return 1
  fi
  if [[ ! -x "$OPB_TOOL_BIN" ]]; then
    say "opbtool binary not found at $OPB_TOOL_BIN"
    return 1
  fi
  "$OPB_TOOL_BIN" -mode inspect -snapshot-dir "$SNAPSHOT_DIR" -snapshot-id "$sid"
}

verify_incremental_manifest() {
  local manifest=${SNAPSHOT_DIR}/"${1}"/manifest.json
  if [[ ! -f "$manifest" ]]; then
    say "Manifest not found for snapshot $1"
    return 1
  fi
  local new_count
  new_count=$(jq -r '.pebbleIncrementalFiles | length' "$manifest")
  if [[ "$new_count" -le 0 ]]; then
    say "Snapshot $1 is not incremental (new files = $new_count)"
    return 1
  fi
  say "Snapshot $1 incremental files: $new_count"
}

say "Demo: Pebble incremental snapshot shipping"
sid=$(latest_manifest)
if [[ -z "$sid" ]]; then
  say "No snapshots detected. Run OpB with --enable-pebble-phase3 to produce snapshots first."
  exit 0
fi

say "Latest snapshot ID: $sid"
inspect_snapshot "$sid" || true

# Walk manifest chain backwards until base.
current="$sid"
chain=()
while [[ -n "$current" ]]; do
  chain+=("$current")
  manifest_file=${SNAPSHOT_DIR}/"$current"/manifest.json
  if [[ ! -f "$manifest_file" ]]; then
    break
  fi
  stype=$(jq -r '.snapshotType' "$manifest_file")
  if [[ "$stype" != "delta" ]]; then
    break
  fi
  parent=$(jq -r '.parentSnapshotId // ""' "$manifest_file")
  current=$parent
done

say "Chain discovered (latest first): ${chain[*]}"

for snapshot_id in "${chain[@]}"; do
  manifest_file=${SNAPSHOT_DIR}/"$snapshot_id"/manifest.json
  stype=$(jq -r '.snapshotType' "$manifest_file")
  if [[ "$stype" == "delta" ]]; then
    verify_incremental_manifest "$snapshot_id" || true
  fi
done

if [[ -f "$LOG_FILE" ]]; then
  say "Recent Pebble snapshot log lines:"
  grep "snapshot: backend=pebble" "$LOG_FILE" | tail -n5 || true
fi

say "Done. Use OPB_BIN=$OPB_BIN (pebble-only mode) with optional --enable-pebble-phase3 for incremental shipping."

