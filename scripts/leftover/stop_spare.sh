#!/usr/bin/env bash
set -euo pipefail

# Stop a spare OpB replica (default B3)

INSTANCE_ID=${1:-B3}

say() { printf "\n\e[1;36m[SPARE]\e[0m %s\n" "$*"; }

say "Stopping spare replica $INSTANCE_ID"
PIDS=$(pgrep -f "opb .*--instance-id $INSTANCE_ID" || true)
if [[ -n "${PIDS:-}" ]]; then
  echo "Killing PIDs: $PIDS"
  kill $PIDS || true
else
  echo "No $INSTANCE_ID PIDs found"
fi
