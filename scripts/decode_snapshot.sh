#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-$ROOT_DIR/snapshots}"
SNAPSHOT_ID="${SNAPSHOT_ID:-${1:-}}"
FORMAT="${FORMAT:-${2:-}}"
SHARDS="${SHARDS:-${3:-}}"
PRETTY="${PRETTY:-1}"

if [[ -z "$SNAPSHOT_DIR" ]]; then
  echo "[decode_snapshot] SNAPSHOT_DIR is required" >&2
  exit 1
fi

ARGS=(
  --snapshot-dir "$SNAPSHOT_DIR"
)

if [[ -n "$SNAPSHOT_ID" ]]; then
  ARGS+=(--snapshot-id "$SNAPSHOT_ID")
fi

if [[ -n "$FORMAT" ]]; then
  ARGS+=(--format "$FORMAT")
fi

if [[ -n "$SHARDS" ]]; then
  ARGS+=(--shards "$SHARDS")
fi

if [[ "$PRETTY" == "0" ]]; then
  ARGS+=(--pretty=false)
fi

echo "[decode_snapshot] go run ./cmd/tools/decodesnapshot ${ARGS[*]}"
cd "$ROOT_DIR"
go run ./cmd/tools/decodesnapshot "${ARGS[@]}"

