#!/usr/bin/env bash
set -euo pipefail

# Graceful shutdown: pause -> snapshot -> commit txn -> shutdown
: "${HTTP_URL:=http://127.0.0.1:8089}"

if command -v curl >/dev/null 2>&1; then
  echo "[opb-shutdown] pause"
  curl -fsS -X POST "$HTTP_URL/admin/pause" -d '{}' || true
  echo "[opb-shutdown] snapshot"
  curl -fsS -X POST "$HTTP_URL/admin/snapshot" -d '{"reason":"shutdown"}' || true
  echo "[opb-shutdown] commit tx"
  curl -fsS -X POST "$HTTP_URL/admin/commit-tx" -d '{}' || true
  echo "[opb-shutdown] shutdown"
  curl -fsS -X POST "$HTTP_URL/admin/shutdown" -d '{}' || true
else
  echo "[opb-shutdown] curl not found; skipping admin calls"
fi


