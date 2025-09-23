#!/usr/bin/env bash
set -euo pipefail

# Usage: scripts/failure-inject.sh './bin/opb --kafka-bootstrap localhost:19092 --input-source kafka --topic-enriched p1.orders.enriched --manifest-source kafka --changelog-source kafka --topic-changelog p2.opb-changelog --topic-snapshots p2.opb-snapshots'

CMD=${1:-}
if [ -z "$CMD" ]; then
  echo "usage: $0 '<command to run>'" >&2
  exit 1
fi

for i in 1 2 3; do
  echo "Run $i..."
  start=$(date +%s)
  bash -lc "$CMD" & pid=$!
  sleep 1
  kill -9 $pid || true
  # restart and wait briefly
  bash -lc "$CMD" & pid=$!
  sleep 5
  end=$(date +%s)
  echo "TTR run $i: $((end-start))s"
  kill $pid || true
done


