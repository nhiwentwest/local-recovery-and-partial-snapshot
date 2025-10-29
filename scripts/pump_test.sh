#!/usr/bin/env bash
set -euo pipefail

# Mục đích: bơm workload lớn vào Kafka một cách an toàn, tránh nghẽn I/O của console-producer.
# Cách dùng:
#   TOPIC=p2.orders N=5000 CHUNK=500 SLEEP=0.1 ./scripts/pump_test.sh
# Tuỳ chọn:
#   MODE=raw|enriched (mặc định raw)
#   PARALLEL=K (số luồng bơm song song, mặc định 1)
#   KEYPREFIX=A (tiền tố storeId khi MODE=enriched)

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
TOPIC=${TOPIC:-p2.orders}
N=${N:-5000}
CHUNK=${CHUNK:-500}
SLEEP=${SLEEP:-0.1}
MODE=${MODE:-raw}
PARALLEL=${PARALLEL:-1}
KEYPREFIX=${KEYPREFIX:-A}
KAFKA_PRODUCER=${KAFKA_PRODUCER:-/opt/homebrew/bin/kafka-console-producer}

say() { printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

# Sinh dòng key|value (enriched) để phân phối khoá đều partitions
# worker_offset giúp mỗi luồng có dải key khác nhau
kv_line_enriched() {
  local i=$1; local worker_offset=${2:-0}
  local pid=$(( (i + worker_offset - 1) % 100 + 1 ))
  local now=$(date +%s)
  local win=$(( (now/10)*10 ))
  local store="$KEYPREFIX"
  local key="${store}#p${pid}#${win}"
  local val
  val=$(printf '{"orderId":"tp-%s","productId":"p%s","price":10000,"qty":1,"storeId":"%s","ts":%d,"validated":true,"normTs":%d}' \
    "$i" "$pid" "$store" "$((win+1))" "$((win+1))")
  printf '%s|%s\n' "$key" "$val"
}

# Sinh dòng value (raw orders)
val_line_raw() {
  local i=$1
  local pid=$(( (i-1) % 100 + 1 ))
  local sid=$(( (i-1) % 5 ))
  local store="A"; case $sid in 1) store="B";; 2) store="C";; 3) store="D";; 4) store="E";; esac
  printf '{"orderId":"tp-%s","productId":"p%s","price":10000,"qty":1,"storeId":"%s","ts":%s}\n' \
    "$i" "$pid" "$store" $((1694508000 + i))
}

produce_chunk() {
  local from=$1; local to=$2; local worker_offset=${3:-0}
  local tmpf
  tmpf=$(mktemp /tmp/pump_chunk.XXXXXX)
  if [[ "$MODE" == "enriched" ]]; then
    for ((j=from;j<=to;j++)); do kv_line_enriched "$j" "$worker_offset" >> "$tmpf"; done
    ${KAFKA_PRODUCER} --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
      --producer-property linger.ms=5 \
      --producer-property batch.size=1048576 \
      --producer-property compression.type=lz4 \
      --producer-property max.block.ms=5000 \
      --producer-property request.timeout.ms=5000 \
      --request-required-acks 1 \
      --property parse.key=true --property key.separator='|' \
      < "$tmpf" >/dev/null 2>&1 || true
  else
    for ((j=from;j<=to;j++)); do val_line_raw "$j" >> "$tmpf"; done
    ${KAFKA_PRODUCER} --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
      --producer-property linger.ms=5 \
      --producer-property batch.size=1048576 \
      --producer-property compression.type=lz4 \
      --producer-property max.block.ms=5000 \
      --producer-property request.timeout.ms=5000 \
      --request-required-acks 1 \
      < "$tmpf" >/dev/null 2>&1 || true
  fi
  rm -f "$tmpf" || true
}

run_worker() {
  local idx=$1
  local start=$2
  local end=$3
  local worker_offset=$(( idx * 1000 ))
  local i=$start
  while (( i <= end )); do
    local bound=$(( i + CHUNK - 1 ))
    if (( bound > end )); then bound=$end; fi
    produce_chunk "$i" "$bound" "$worker_offset"
    say "worker#$idx progress: ${bound}/${end}"
    sleep "$SLEEP"
    i=$(( bound + 1 ))
  done
}

main() {
  say "pumping N=${N} to topic=${TOPIC} (chunk=${CHUNK}, sleep=${SLEEP}s, mode=${MODE}, parallel=${PARALLEL})"
  if (( PARALLEL <= 1 )); then
    run_worker 0 1 "$N"
  else
    local per=$(( (N + PARALLEL - 1) / PARALLEL ))
    for ((w=0; w<PARALLEL; w++)); do
      local s=$(( w*per + 1 ))
      local e=$(( s + per - 1 ))
      if (( e > N )); then e=$N; fi
      run_worker "$w" "$s" "$e" &
    done
    wait
  fi
  say "done."
}

main "$@"


