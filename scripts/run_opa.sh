#!/usr/bin/env bash
set -euo pipefail
# [CỬA SỔ 2] - OpA (Data Normalizer)
# Nhiệm vụ: Tự chờ topic, chạy test "exactly-once", sau đó chạy nền để xử lý dữ liệu.

# ==========================
# Config
# ==========================
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
PREFIX=${PREFIX:-p2}
OPA_GROUP=${OPA_GROUP:-opa-g}
HTTP_OPA=${HTTP_OPA:-:8088}
OPA_TX_ID=${OPA_TX_ID:-opa-tx-1}
KAFKA_TOPICS=${KAFKA_TOPICS:-/opt/homebrew/bin/kafka-topics}
KAFKA_PRODUCER=${KAFKA_PRODUCER:-/opt/homebrew/bin/kafka-console-producer}
KAFKA_CONSUMER=${KAFKA_CONSUMER:-/opt/homebrew/bin/kafka-console-consumer}
CURL=${CURL:-curl}
BIN_OPA=${BIN_OPA:-./bin/opa}

TOPIC_RAW=${PREFIX}.orders
TOPIC_IN=${PREFIX}.orders.enriched

say() { printf "[OpA] [%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

wait_for_topics() {
  say "Waiting for Kafka topic '${TOPIC_RAW}' to be created..."
  for i in {1..60}; do
    if ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC_RAW}" >/dev/null 2>&1; then
      say "Topic '${TOPIC_RAW}' found. Proceeding."
      return 0
    fi
    if [[ $i -eq 1 ]]; then say "(Will wait up to 2 minutes for topic...)"; fi
    sleep 2
  done
  say "ERROR: Timed out waiting for topic. Please run run_infra.sh in another window."
  exit 1
}

wait_http() { local addr=$1; for i in {1..30}; do if ${CURL} -sf "${addr}" >/dev/null 2>&1; then return 0; fi; sleep 0.3; done; return 1; }
start_opa() { pkill opa >/dev/null 2>&1 || true; say "Starting OpA in normal mode..."; ${BIN_OPA} -bootstrap "${BOOTSTRAP}" -group-id "${OPA_GROUP}" -topic-in "${TOPIC_RAW}" -topic-out "${TOPIC_IN}" -tx-id "${OPA_TX_ID}" -http "${HTTP_OPA}" > ./logs/opa.out 2>&1 & sleep 0.5; wait_http "http://127.0.0.1${HTTP_OPA}/healthz" || say "WARN: OpA /healthz not ready"; }
start_opa_mode() { local mode=$1; pkill opa >/dev/null 2>&1 || true; say "Starting OpA with crash-mode=${mode}"; ${BIN_OPA} -bootstrap "${BOOTSTRAP}" -group-id "${OPA_GROUP}" -topic-in "${TOPIC_RAW}" -topic-out "${TOPIC_IN}" -tx-id "${OPA_TX_ID}" -crash-mode "${mode}" -http "${HTTP_OPA}" > ./logs/opa.out 2>&1 & sleep 0.5; wait_http "http://127.0.0.1${HTTP_OPA}/healthz" || true; }
produce_raw_one() { printf '{"orderId":"%s","productId":"%s","price":%s,"qty":%s,"storeId":"%s","ts":%s}\n' "$1" "${2:-p1}" "${3:-10000}" "${4:-1}" "${5:-A}" "${6:-1694505000}" | ${KAFKA_PRODUCER} --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC_RAW" >/dev/null 2>&1 || true; }
find_in_topic_by_order() { ${KAFKA_CONSUMER} --bootstrap-server "$BOOTSTRAP" --topic "$1" --from-beginning --timeout-ms "${3:-15000}" --max-messages 1000 --isolation-level read_committed --property print.key=true --property print.value=true --property key.separator='|' 2>/dev/null | grep -F "$2" || true; }

test_crash_matrix_opa() {
  say "Testing OpA EOS crash matrix (before/mid/after)..."
  RUN=$$-$RANDOM
  start_opa_mode before; produce_raw_one "cb1_${RUN}"; sleep 2; pkill opa >/dev/null 2>&1 || true; start_opa; out=$(find_in_topic_by_order "$TOPIC_IN" "cb1_${RUN}" 30000); cnt=$(echo "$out" | wc -l | awk '{print $1}'); if [[ "$cnt" -eq 1 ]]; then say "[PASS] OpA crash BEFORE"; else say "[FAIL] OpA crash BEFORE: count=$cnt"; fi
  start_opa_mode mid; produce_raw_one "cm1_${RUN}"; sleep 2; pkill opa >/dev/null 2>&1 || true; start_opa; out=$(find_in_topic_by_order "$TOPIC_IN" "cm1_${RUN}" 30000); cnt=$(echo "$out" | wc -l | awk '{print $1}'); if [[ "$cnt" -eq 1 ]]; then say "[PASS] OpA crash MID"; else say "[FAIL] OpA crash MID: count=$cnt"; fi
  start_opa_mode after; produce_raw_one "ca1_${RUN}"; sleep 2; pkill opa >/dev/null 2>&1 || true; start_opa; out=$(find_in_topic_by_order "$TOPIC_IN" "ca1_${RUN}" 30000); cnt=$(echo "$out" | wc -l | awk '{print $1}'); if [[ "$cnt" -eq 1 ]]; then say "[PASS] OpA crash AFTER"; else say "[FAIL] OpA crash AFTER: count=$cnt"; fi
}

main() {
  wait_for_topics
  test_crash_matrix_opa
  say "Crash matrix test finished. Starting OpA in normal mode to serve OpB tests."
  start_opa
  say "OpA is running in the background. This window will now wait indefinitely."
  # Vòng lặp vô hạn để giữ script sống, đảm bảo process con opa không bị kill
  while true; do sleep 60; done
}

main "$@"