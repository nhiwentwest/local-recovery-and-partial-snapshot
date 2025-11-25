#!/usr/bin/env bash
set -euo pipefail

# Start a spare OpB replica (B3) on port :8091

HTTP_OPB1=${OPB1_HTTP:-http://127.0.0.1:8089}
HTTP_OPB2=${OPB2_HTTP:-http://127.0.0.1:8090}
HTTP_OPB3=${OPB3_HTTP:-http://127.0.0.1:8091}
INSTANCE_ID=${1:-B3}
PORT=${2:-8091}

say() { printf "\n\e[1;36m[SPARE]\e[0m %s\n" "$*"; }

say "Starting spare replica $INSTANCE_ID on :$PORT"
OPB_PEERS="$HTTP_OPB1,$HTTP_OPB2,$HTTP_OPB3" \
./bin/opb --state-backend memory --kafka-bootstrap 127.0.0.1:9092 \
  --group-id opb-standalone --input-source kafka \
  --topic-enriched p1.orders.enriched --output-topic p1.orders.output \
  --changelog-sink both --manifest-sink both \
  --topic-changelog p1.opb-changelog --topic-snapshots p1.opb-snapshots \
  --window-size 60 --tx-batch-size 1000 --tx-linger-ms 100 \
  --heartbeat-interval-ms 2000 \
  --http :$PORT --instance-id "$INSTANCE_ID" \
  > ./logs/${INSTANCE_ID}.out 2>&1 &

say "$INSTANCE_ID started. Check health: curl -sf http://127.0.0.1:$PORT/healthz"
