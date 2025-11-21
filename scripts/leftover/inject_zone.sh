#!/usr/bin/env bash
set -euo pipefail
# Helper: inject test data via OpB HTTP API
# Usage:
#   HTTP=http://127.0.0.1:8089 STORE=A- N=1000 MODE=new ./scripts/inject_zone.sh
#   HTTP=http://127.0.0.1:8089 STORE=A- MODE=duplicate N=100 ./scripts/inject_zone.sh
# Optional: PRODUCT=p1 WS=1694499900

HTTP=${HTTP:-http://127.0.0.1:8089}
STORE=${STORE:-A-}
N=${N:-1000}
MODE=${MODE:-new}
PRODUCT=${PRODUCT:-}
WS=${WS:-}

body=$(jq -n \
  --arg store "$STORE" \
  --arg mode "$MODE" \
  --argjson n "$N" \
  --arg product "$PRODUCT" \
  --arg ws "$WS" \
  '{storeId:$store, mode:$mode, n:$n} + ( ($product|length)>0 ? {productId:$product} : {} ) + ( ($ws|length)>0 ? {ws:($ws|tonumber)} : {} )')

echo "POST $HTTP/api/inject-test-data -> $body"
curl -sS -X POST -H 'Content-Type: application/json' -d "$body" "$HTTP/api/inject-test-data" | jq -r .

