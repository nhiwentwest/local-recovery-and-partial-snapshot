#!/usr/bin/env bash
set -euo pipefail

# Poll /metrics và in subset quan trọng phục vụ throughput/ổn định
# Cách dùng: HTTP=:8089 DURATION=60 INTERVAL=2 ./scripts/monitor_metrics.sh

HTTP=${HTTP:-http://127.0.0.1:8089/metrics}
DURATION=${DURATION:-60}
INTERVAL=${INTERVAL:-2}

say(){ printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

end=$(( $(date +%s) + DURATION ))
say "monitoring metrics from $HTTP for ${DURATION}s (every ${INTERVAL}s)"
while (( $(date +%s) < end )); do
  curl -fsS "$HTTP" 2>/dev/null \
   | egrep -E 'opb_tx_produced_total|opb_tx_aborted_total|opb_partition_lag|opb_ttr_seconds|opb_tx_batch_duration_seconds' \
   | sed 's/^/  /'
  sleep "$INTERVAL"
done
say "done."


