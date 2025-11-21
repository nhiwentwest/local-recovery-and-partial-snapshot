#!/usr/bin/env bash
set -euo pipefail

# Bơm data enriched (keyed) trực tiếp vào p1.orders.enriched bằng Go pump

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
TOPIC=${TOPIC:-p1.orders.enriched}
N=${N:-10000}
STORES=${STORES:-"A-,B-,C-,D-,E-,F-,G-,H-,I-,J-,K-,L-,M-,N-,O-,P-"}
PARALLEL=${PARALLEL:-4}
WINDOW_SIZE=${WINDOW_SIZE:-60}

say() { printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

  say "Building pump..."
  go build -o bin/pump ./cmd/pump

RANDOM_DELAY_MS_MIN=${RANDOM_DELAY_MS_MIN:-0}
RANDOM_DELAY_MS_MAX=${RANDOM_DELAY_MS_MAX:-100}
BATCH=${BATCH:-1000}
if [ "$BATCH" -le 0 ]; then BATCH=1000; fi

say "Pumping enriched data: N=${N} to topic=${TOPIC} (parallel=${PARALLEL}, stores=${STORES}, batch=${BATCH}, delay=${RANDOM_DELAY_MS_MIN}-${RANDOM_DELAY_MS_MAX}ms)"

remaining=$N
iter=0
OFFSET_START=${OFFSET_START:-1}
if [ "$OFFSET_START" -lt 1 ]; then OFFSET_START=1; fi
current_start=$OFFSET_START
while [ "$remaining" -gt 0 ]; do
  iter=$((iter+1))
  chunk=$BATCH
  if [ "$chunk" -gt "$remaining" ]; then chunk=$remaining; fi
  say "Batch ${iter}: sending ${chunk} messages (remaining=$((remaining-chunk)))"
./bin/pump \
  -bootstrap "$BOOTSTRAP" \
  -topic "$TOPIC" \
  -mode enriched \
    -n "$chunk" \
  -parallel "$PARALLEL" \
  -stores "$STORES" \
  -window-size "$WINDOW_SIZE" \
  -start-idx "$current_start"
  remaining=$((remaining-chunk))
  current_start=$((current_start + chunk))
  if [ "$remaining" -le 0 ]; then break; fi
  # random sleep between batches
  if [ "$RANDOM_DELAY_MS_MAX" -gt 0 ]; then
    span=$((RANDOM_DELAY_MS_MAX - RANDOM_DELAY_MS_MIN))
    if [ "$span" -lt 0 ]; then span=0; fi
    r=$RANDOM
    if [ "$span" -gt 0 ]; then
      delay_ms=$((RANDOM_DELAY_MS_MIN + (r % (span+1))))
    else
      delay_ms=$RANDOM_DELAY_MS_MIN
    fi
    delay_sec=$(awk -v ms="$delay_ms" 'BEGIN{printf "%.3f", ms/1000}')
    say "Sleeping ${delay_ms}ms before next batch..."
    sleep "$delay_sec"
  fi
done

say "done."
