#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")"/.. && pwd)"
cd "$ROOT_DIR"

PORT=${PORT:-9095}
echo "[obs] Starting Prometheus on :${PORT} using prometheus.yml"
pkill -f "prometheus --config.file=./prometheus.yml" >/dev/null 2>&1 || true
if ! command -v prometheus >/dev/null 2>&1; then
  echo "[obs] Prometheus binary not found. On macOS, install via: brew install prometheus"
  exit 1
fi
nohup prometheus --config.file=./prometheus.yml --web.listen-address=":${PORT}" \
  > /tmp/prometheus.out 2>&1 &
sleep 0.5
echo "[obs] Prometheus at http://localhost:${PORT}"

echo "[obs] Reminder: point Grafana to Prometheus URL http://host.docker.internal:${PORT} or http://localhost:${PORT} depending on your Grafana runtime"

