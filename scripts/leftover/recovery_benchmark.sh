#!/usr/bin/env bash
set -euo pipefail

# Usage: CHANGELOG=${CHANGELOG:-on|off} DURATION=${DURATION:-30} ./scripts/recovery_benchmark.sh

CHANGELOG=${CHANGELOG:-on}
DURATION=${DURATION:-30}
HTTP=${HTTP:-http://127.0.0.1:8080/metrics}

echo "[recovery_benchmark] changelog=$CHANGELOG duration=${DURATION}s"

start_ts=$(date +%s)
bytes_before=$(curl -s "$HTTP" | awk -F' ' '/^opb_replay_bytes_total/ {print $2}' | tail -n1)
bytes_before=${bytes_before:-0}

echo "[recovery_benchmark] Simulating OpB restart..."
pkill opb || true
sleep 1

# User should start OpB externally with desired options; we only watch metrics window.
echo "[recovery_benchmark] Waiting ${DURATION}s to observe recovery metrics..."
sleep "$DURATION"

bytes_after=$(curl -s "$HTTP" | awk -F' ' '/^opb_replay_bytes_total/ {print $2}' | tail -n1)
bytes_after=${bytes_after:-0}
ttr=$(curl -s "$HTTP" | awk -F' ' '/^opb_recovery_ttr_seconds/ {print $2}' | tail -n1)

echo "[recovery_benchmark] replay_bytes_delta=$(( ${bytes_after%.*} - ${bytes_before%.*} ))"
echo "[recovery_benchmark] last_TTR_seconds=${ttr:-N/A}"

end_ts=$(date +%s)
echo "[recovery_benchmark] done in $(( end_ts - start_ts ))s"


