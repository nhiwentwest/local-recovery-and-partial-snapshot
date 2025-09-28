#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

pkill -f "bin/opb" >/dev/null 2>&1 || true
pkill -f load-benchmark.sh >/dev/null 2>&1 || true

docker-compose down >/dev/null 2>&1 || true

echo "Stopped all opb processes and docker containers."
