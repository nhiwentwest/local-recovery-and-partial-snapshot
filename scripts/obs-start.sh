#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")"/.. && pwd)"
cd "$ROOT_DIR"

echo "[obs] Starting Prometheus on :9091 using prometheus.yml"
pkill -f "prometheus --config.file=./prometheus.yml" >/dev/null 2>&1 || true
nohup prometheus --config.file=./prometheus.yml --web.listen-address=":9091" \
  > /tmp/prometheus.out 2>&1 &
sleep 0.5
echo "[obs] Prometheus at http://localhost:9091"

echo "[obs] Reminder: point Grafana to Prometheus URL http://host.docker.internal:9091 or http://localhost:9091 depending on your Grafana runtime"

