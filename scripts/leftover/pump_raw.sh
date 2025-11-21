#!/usr/bin/env bash
set -euo pipefail

# Bơm raw orders vào p1.orders (OpA sẽ normalize → p1.orders.enriched) bằng Go pump (không cần Kafka CLI)

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
TOPIC=${TOPIC:-p1.orders}
N=${N:-10000}
STORES=${STORES:-"A-,B-,C-,D-,E-,F-,G-,H-,I-,J-,K-,L-,M-,N-,O-,P-"}
PARALLEL=${PARALLEL:-4}

say() { printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

if [ ! -x ./bin/pump ]; then
  say "Building pump..."
  go build -o bin/pump ./cmd/pump
fi

say "Pumping raw orders: N=${N} to topic=${TOPIC} (parallel=${PARALLEL}, stores=${STORES})"
./bin/pump \
  -bootstrap "$BOOTSTRAP" \
  -topic "$TOPIC" \
  -mode raw \
  -n "$N" \
  -parallel "$PARALLEL" \
  -stores "$STORES"

say "done."
