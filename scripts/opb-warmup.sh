#!/usr/bin/env bash
set -euo pipefail

# Warm up page cache for state-dir and snapshot-dir to reduce cold-start latency.
# Usage: scripts/opb-warmup.sh <STATE_DIR> <SNAPSHOT_DIR>

STATE_DIR="${1:-./data/opb}"
SNAP_DIR="${2:-./snapshots}"

if command -v vmtouch >/dev/null 2>&1; then
  vmtouch -t -m 2G "$STATE_DIR" || true
  vmtouch -t -m 1G "$SNAP_DIR" || true
else
  find "$STATE_DIR" -type f -print0 2>/dev/null | xargs -0 -n1 -P4 dd if=/dev/stdin of=/dev/null bs=4M status=none 2>/dev/null || true
  find "$SNAP_DIR"  -type f -print0 2>/dev/null | xargs -0 -n1 -P2 dd if=/dev/stdin of=/dev/null bs=4M status=none 2>/dev/null || true
fi

echo "[warmup] done: STATE_DIR=$STATE_DIR SNAP_DIR=$SNAP_DIR"


