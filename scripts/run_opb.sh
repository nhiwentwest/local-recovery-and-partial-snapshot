#!/usr/bin/env bash
set -euo pipefail
# [CỬA SỔ 3] - OpB (Stateful Aggregator)
# Nhiệm vụ: Tự chờ dependencies (Kafka, OpA), sau đó chạy toàn bộ các bài test chính.

# ==========================
# Config
# ==========================
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
PREFIX=${PREFIX:-p2}
GROUP=${GROUP:-opb-g-$$-$RANDOM}
HTTP=${HTTP:-:8089}
HTTP_OPA=${HTTP_OPA:-:8088}
STATE_DIR=${STATE_DIR:-./data/opb-demo}
SNAPSHOT_DIR=${SNAPSHOT_DIR:-./snapshots}
TX_ID=${TX_ID:-opb-tx-1}
SNAPSHOT_INTERVAL=${SNAPSHOT_INTERVAL:-5}
WINDOW_SIZE=${WINDOW_SIZE:-10}
HEAVY_N=${HEAVY_N:-5000}
THR_WINDOW_SEC=${THR_WINDOW_SEC:-60}
ENABLE_AB=${ENABLE_AB:-1}
TX_BATCH_SIZE=${TX_BATCH_SIZE:-100}
TX_LINGER_MS=${TX_LINGER_MS:-100}

KAFKA_TOPICS=${KAFKA_TOPICS:-/opt/homebrew/bin/kafka-topics}
KAFKA_PRODUCER=${KAFKA_PRODUCER:-/opt/homebrew/bin/kafka-console-producer}
KAFKA_CONSUMER=${KAFKA_CONSUMER:-/opt/homebrew/bin/kafka-console-consumer}
CURL=${CURL:-curl}
BIN_OPB=${BIN_OPB:-./bin/opb}

TOPIC_RAW=${PREFIX}.orders
TOPIC_IN=${PREFIX}.orders.enriched
TOPIC_OUT=${PREFIX}.orders.output
TOPIC_CHANGELOG=${PREFIX}.opb-changelog
TOPIC_SNAPSHOTS=${PREFIX}.opb-snapshots

# --- Helper Functions ---
say() { printf "[OpB] [%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

# Get partition count for a topic (default 6 if parsing fails)
partition_count() {
  local topic=$1
  local cnt=$( ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --describe --topic "${topic}" 2>/dev/null | awk -F"partition(s)" '/PartitionCount/ {print $1}' | awk -F"PartitionCount:" '{print $2}' | awk '{print $1}' )
  if [[ -z "$cnt" ]]; then echo 6; else echo "$cnt"; fi
}

# Compute Kafka partition for a key using Murmur2 to match default partitioner
partition_for_key() {
  local key=$1
  local topic=$2
  local parts=$(partition_count "$topic")
  go run ./scripts/hash_murmur2.go "$key" "$parts"
}

# Start a consumer pinned to a specific partition from its latest offset
start_consumer_partition() {
  local topic=$1 part=$2 outfile=$3 t=${4:-8000}
  ( ${KAFKA_CONSUMER} --bootstrap-server "$BOOTSTRAP" --topic "$topic" --partition "$part" --offset latest --timeout-ms "$t" --max-messages 10000 --isolation-level read_committed 2>/dev/null --property print.key=true --property print.value=true --property key.separator='|' > "$outfile" ) & echo $!
}

wait_for_dependencies() {
  say "Waiting for Kafka topic '${TOPIC_IN}'..."
  for i in {1..60}; do
    if ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC_IN}" >/dev/null 2>&1; then
      say "Topic '${TOPIC_IN}' found."
      break
    fi
    if [[ $i -eq 1 ]]; then say "(Will wait up to 2 minutes for topic...)"; fi
    if [[ $i -eq 60 ]]; then say "ERROR: Timed out waiting for topic ${TOPIC_IN}."; exit 1; fi
    sleep 2
  done

  local opa_health_addr="http://127.0.0.1${HTTP_OPA}/healthz"
  say "Waiting for OpA service at ${opa_health_addr}..."
  for i in {1..60}; do
    if ${CURL} -sf "${opa_health_addr}" >/dev/null 2>&1; then
      say "OpA service is healthy. Proceeding."
      return 0
    fi
    if [[ $i -eq 1 ]]; then say "(Will wait up to 2 minutes for OpA...)"; fi
    if [[ $i -eq 60 ]]; then say "ERROR: Timed out waiting for OpA service."; exit 1; fi
    sleep 2
  done
}

wait_http() { for i in {1..30}; do if ${CURL} -sf "http://127.0.0.1${HTTP}/healthz" >/dev/null 2>&1; then return 0; fi; sleep 0.3; done; return 1; }
clean_state() { say "Cleaning state and snapshots"; rm -rf "${STATE_DIR}" "${SNAPSHOT_DIR}"; mkdir -p "${STATE_DIR}" "${SNAPSHOT_DIR}"; }
stop_all() { pkill opb >/dev/null 2>&1 || true; }
start_opb() { stop_all; say "Starting OpB..."; local GID="opb-g-$$-$RANDOM"; ${BIN_OPB} --state-backend pebble --state-dir "${STATE_DIR}" --snapshot-dir "${SNAPSHOT_DIR}" --kafka-bootstrap "${BOOTSTRAP}" --group-id "${GID}" --input-source kafka --topic-enriched "${TOPIC_IN}" --output-topic "${TOPIC_OUT}" --changelog-sink both --manifest-sink both --topic-changelog "${TOPIC_CHANGELOG}" --topic-snapshots "${TOPIC_SNAPSHOTS}" --window-size "${WINDOW_SIZE}" --snapshot-interval "${SNAPSHOT_INTERVAL}" --output-tx-id "${TX_ID}" --tx-batch-size "${TX_BATCH_SIZE}" --tx-linger-ms "${TX_LINGER_MS}" --http "${HTTP}" > ./logs/opb_kafka.out 2>&1 & sleep 0.8; if wait_http; then say "OpB ready"; else say "ERROR: OpB not ready"; exit 1; fi; }
start_opb_with() { local sink=$1; stop_all; say "Starting OpB with changelog-sink=${sink}"; ${BIN_OPB} --state-backend pebble --state-dir "${STATE_DIR}" --snapshot-dir "${SNAPSHOT_DIR}" --kafka-bootstrap "${BOOTSTRAP}" --group-id "${GROUP}" --input-source kafka --topic-enriched "${TOPIC_IN}" --output-topic "${TOPIC_OUT}" --changelog-sink "${sink}" --manifest-sink both --topic-changelog "${TOPIC_CHANGELOG}" --topic-snapshots "${TOPIC_SNAPSHOTS}" --window-size "${WINDOW_SIZE}" --snapshot-interval "${SNAPSHOT_INTERVAL}" --output-tx-id "${TX_ID}" --tx-batch-size "${TX_BATCH_SIZE}" --tx-linger-ms "${TX_LINGER_MS}" --http "${HTTP}" > ./logs/opb_kafka.out 2>&1 & sleep 0.8; wait_http || say "WARN: OpB not ready"; }
pump_via_script() { local n=${1} chunk=${2:-500} gap=${3:-0.05}; local script_dir; script_dir=$(cd "$(dirname "$0")" && pwd); say "Pumping N=${n} to topic=${TOPIC_RAW} (via pump_test.sh)"; TOPIC="$TOPIC_RAW" N="$n" CHUNK="$chunk" SLEEP="$gap" BOOTSTRAP="$BOOTSTRAP" "$script_dir/pump_test.sh"; }
produce_raw_one() { printf '{"orderId":"%s","productId":"%s","price":%s,"qty":%s,"storeId":"%s","ts":%s}\n' "$1" "${2:-p1}" "${3:-10000}" "${4:-1}" "${5:-A}" "${6:-1694505000}" | ${KAFKA_PRODUCER} --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC_RAW" >/dev/null 2>&1 || true; }
# For latency samples, write directly to enriched to avoid dependency on OpA
produce_enriched_one() { printf '{"orderId":"%s","productId":"%s","price":%s,"qty":%s,"storeId":"%s","ts":%s,"validated":true,"normTs":%s}\n' "$1" "${2:-p1}" "${3:-10000}" "${4:-1}" "${5:-A}" "$6" "$6" | ${KAFKA_PRODUCER} --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC_IN" >/dev/null 2>&1 || true; }
newest_snapshot_id() { ls -1 "${SNAPSHOT_DIR}" 2>/dev/null | grep -E '^[0-9T:-]+Z$' | sort | tail -1; }
snapshot_size_kb() { local id=$1; local dir="${SNAPSHOT_DIR}/${id}"; if [[ -d "$dir" ]]; then du -sk "$dir" 2>/dev/null | awk '{print $1}'; else echo 0; fi; }
wait_new_snapshot() { local prev=$1; local timeout=${2:-30}; say "[SNAPSHOT] Waiting for new snapshot (prev=${prev}, timeout=${timeout}s)"; for i in $(seq 1 $timeout); do local cur=$(newest_snapshot_id || true); if [[ -n "$cur" && "$cur" != "$prev" ]]; then say "[SNAPSHOT] New snapshot found: ${cur}"; echo "$cur"; return 0; fi; sleep 1; done; say "[SNAPSHOT] Timeout waiting for new snapshot."; echo "$prev"; }
start_consumer_bg_topic() { local topic=$1 outfile=$2 t=${3:-8000}; local gid=lat-$$-$RANDOM; ( ${KAFKA_CONSUMER} --bootstrap-server "$BOOTSTRAP" --topic "$topic" --group "$gid" --consumer-property auto.offset.reset=latest --timeout-ms "$t" --max-messages 10000 --isolation-level read_committed 2>/dev/null --property print.key=true --property print.value=true --property key.separator='|' > "$outfile" ) & echo $!; }
wait_key_in_file() { local key=$1 outfile=$2 secs=${3:-8}; for i in $(seq 1 $secs); do if grep -q "^${key}|" "$outfile" 2>/dev/null; then return 0; fi; sleep 1; done; return 1; }

# --- Main Test Functions ---
measure_ttr_3x() { say "Measuring TTR x3..."; TTRS=(); for i in 1 2 3; do stop_all; T0=$(date +%s); start_opb; produce_raw_one "ttr$i"; ${KAFKA_CONSUMER} --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC_OUT" --group ttr-$$-$RANDOM --from-beginning --timeout-ms 15000 --max-messages 1 >/dev/null 2>&1 || true; T1=$(date +%s); D=$((T1-T0)); say "TTR$i=${D}s"; TTRS+=("$D"); done; MIN=${TTRS[0]}; MAX=${TTRS[0]}; SUM=0; for v in "${TTRS[@]}"; do (( v < MIN )) && MIN=$v; (( v > MAX )) && MAX=$v; SUM=$((SUM+v)); done; AVG=$(awk -v s="$SUM" -v n="${#TTRS[@]}" 'BEGIN{printf "%.3f", s/n}'); say "TTR Result: min=${MIN}s avg=${AVG}s max=${MAX}s"; }
measure_latency_throughput() {
  # Stabilize before measuring
  local STABILIZE_SLEEP=${STABILIZE_SLEEP:-5}
  say "Stabilizing ${STABILIZE_SLEEP}s before latency measurement..."; sleep "$STABILIZE_SLEEP"

  say "Measuring Latency..."
  local N_SAMPLES=${N_SAMPLES:-8}
  local LAT_TIMEOUT_MS=${LAT_TIMEOUT_MS:-30000}
  LATS=()

  local WIN=${WINDOW_SIZE}

  for i in $(seq 1 ${N_SAMPLES}); do
    # Recompute next window each sample to tránh lệch mốc
    base=$(date +%s)
    next_win=$(( (base / WIN + 1) * WIN ))
    pid="pL${i}"
    oid=lt${i}_$RANDOM
    ts=$((next_win + 3))
    key="A#${pid}#${next_win}"

    # Align close to next window if còn xa
    now=$(date +%s)
    if (( ts - now > 1 )); then sleep $((ts - now - 1)); fi

    T0=$(date +%s)
    out_file="/tmp/lat_${i}.txt"; rm -f "$out_file"
    # Đo trên changelog (1 partition, visibility tức thời trong cùng transaction)
    cpid=$(start_consumer_bg_topic "$TOPIC_CHANGELOG" "$out_file" ${LAT_TIMEOUT_MS})
    sleep 0.5
    # Warm pipeline: burst 3 events for the same key
    produce_enriched_one "$oid"   "$pid" 9000 1 A "$ts"
    sleep 0.05
    produce_enriched_one "${oid}b" "$pid" 9000 1 A "$ts"
    sleep 0.05
    produce_enriched_one "${oid}c" "$pid" 9000 1 A "$ts"

    say "Latency sample ${i}/${N_SAMPLES}: waiting for key=${key} (topic=${TOPIC_CHANGELOG}, timeout=${LAT_TIMEOUT_MS}ms)"
    if wait_key_in_file "$key" "$out_file" $((LAT_TIMEOUT_MS/1000)); then
      T1=$(date +%s); LATS+=( $((T1-T0)) ); say "Latency sample ${i}: HIT in $((T1-T0))s"
    else
      LATS+=( $((LAT_TIMEOUT_MS/1000)) ); say "Latency sample ${i}: MISS"
    fi
    kill "$cpid" >/dev/null 2>&1 || true
  done

  printf "%s\n" "${LATS[@]}" | sort -n > /tmp/lats.txt
  COUNT=$(wc -l < /tmp/lats.txt | awk '{print $1}')
  P50=$(awk -v n=$COUNT 'NR==int((n+1)*0.50){print; exit}' /tmp/lats.txt)
  P95=$(awk -v n=$COUNT 'NR==int((n+1)*0.95){print; exit}' /tmp/lats.txt)
  P99=$(awk -v n=$COUNT 'NR==int((n+1)*0.99){print; exit}' /tmp/lats.txt)
  say "Latency Result: p50=${P50}s p95=${P95}s p99=${P99}s (n=${COUNT})"

  say "Measuring Throughput..."
  if [[ ${HEAVY_N:-0} -ge 10000 && ${THR_WINDOW_SEC:-0} -lt 70 ]]; then THR_WINDOW_SEC=70; fi
  local gid=tp-$$-$RANDOM; rm -f /tmp/tp_out.txt
  say "[THROUGHPUT] START window ~${THR_WINDOW_SEC}s (counting on changelog)"
  (${KAFKA_CONSUMER} --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC_CHANGELOG" --group "$gid" --timeout-ms $((THR_WINDOW_SEC*1000)) --max-messages 2000000 2>/dev/null | grep -v "Processed a total" > /tmp/tp_out.txt) & cpid=$!
  sleep 1; pump_via_script "${HEAVY_N}" 500 0.05
  for i in $(seq 1 $((THR_WINDOW_SEC+5))); do if ! kill -0 "$cpid" >/dev/null 2>&1; then break; fi; sleep 1; done
  if kill -0 "$cpid" >/dev/null 2>&1; then kill "$cpid" >/dev/null 2>&1 || true; fi
  lines=$(wc -l < /tmp/tp_out.txt | awk '{print $1}')
  say "[THROUGHPUT] END count=${lines} (~$((lines/THR_WINDOW_SEC)) msgs/s)"
}
experiment_changelog_ab() { say "[A/B TEST] BEGIN"; say "[A/B][WITH] Setting up..."; clean_state; start_opb_with both; local s0=$(newest_snapshot_id || true); pump_via_script 10000 500 0.03; local idA=$(wait_new_snapshot "$s0" 20); if [[ "$idA" == "$s0" ]]; then produce_raw_one "ab-tick1"; idA=$(wait_new_snapshot "$s0" 10); fi; sleep 5; local idA2=$(newest_snapshot_id || true); [[ -n "$idA2" ]] && idA="$idA2"; local szA=$(snapshot_size_kb "$idA"); say "[A/B][WITH] Counting changelog..."; stop_all; local clogA=$(${KAFKA_CONSUMER} --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC_CHANGELOG" --from-beginning --timeout-ms 20000 --max-messages 200000 2>/dev/null | grep -v "Processed a total" | wc -l | awk '{print $1}'); say "[A/B][WITH] Result: snapshot=${idA} size_kb=${szA} changelog_records~=${clogA}"; say "[A/B][NO] Setting up..."; clean_state; start_opb_with none; local s1=$(newest_snapshot_id || true); pump_via_script 10000 500 0.03; local idB=$(wait_new_snapshot "$s1" 20); if [[ "$idB" == "$s1" ]]; then produce_raw_one "ab-tick2"; idB=$(wait_new_snapshot "$s1" 10); fi; sleep 5; local idB2=$(newest_snapshot_id || true); [[ -n "$idB2" ]] && idB="$idB2"; local szB=$(snapshot_size_kb "$idB"); say "[A/B][NO] Result: snapshot=${idB} size_kb=${szB} changelog_records~=0"; say "[A/B] SUMMARY => with: ${szA}KB vs no: ${szB}KB; records with: ~${clogA} vs no: 0"; say "[A/B TEST] END"; }

main() {
  wait_for_dependencies
  
  say "Running TTR test..."
  measure_ttr_3x
  
  say "Preparing for Latency/Throughput tests..."
  clean_state
  start_opb
  
  say "Running Latency and Throughput tests..."
  measure_latency_throughput

  if [[ "${ENABLE_AB:-0}" -eq 1 ]]; then
    say "Running A/B test..."
    experiment_changelog_ab
  fi

  say "All OpB tests finished. Stopping processes."
  stop_all
}

main "$@"
