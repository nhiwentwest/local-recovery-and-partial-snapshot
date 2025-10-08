#!/usr/bin/env bash
set -euo pipefail

# Recover-then-join launcher for OpB. Does not change any KIPs.
# Expects OpB binary to accept flags used elsewhere in this repo.

: "${KAFKA_BOOTSTRAP:=127.0.0.1:9092}"
: "${GROUP_ID:=opb-g}"
: "${TX_ID:=opb-tx-1}"
: "${TOPIC_PREFIX:=p2}"
: "${SNAPSHOT_DIR:=./snapshots}"
: "${STATE_DIR:=./data/opb}"
: "${HTTP_ADDR:=:8089}"
: "${JOIN_JITTER_SEC:=8}"
: "${BIN:=./bin/opb}"

JITTER=$(( (RANDOM % (JOIN_JITTER_SEC-1)) + 2 ))

echo "[opb-start] warmup page cache"
"$(dirname "$0")/opb-warmup.sh" "$STATE_DIR" "$SNAPSHOT_DIR" || true

echo "[opb-start] launching OpB (normal JOIN)"
exec "$BIN" \
  -kafka-bootstrap "$KAFKA_BOOTSTRAP" \
  -group-id "$GROUP_ID" \
  -topic-prefix "$TOPIC_PREFIX" \
  -snapshot-dir "$SNAPSHOT_DIR" \
  -state-dir "$STATE_DIR" \
  -http "$HTTP_ADDR" \
  -output-tx-id "$TX_ID"


