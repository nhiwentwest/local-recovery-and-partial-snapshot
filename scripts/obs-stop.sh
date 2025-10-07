#!/usr/bin/env bash
set -euo pipefail

echo "[obs] Stopping Prometheus"
pkill -f "prometheus --config.file=./prometheus.yml" >/dev/null 2>&1 || true
echo "[obs] Done"

