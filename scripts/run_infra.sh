#!/usr/bin/env bash
set -euo pipefail
# [CỬA SỔ 1] - INFRASTRUCTURE
# Nhiệm vụ: Xóa và tạo lại các topic Kafka để đảm bảo môi trường sạch.

# ==========================
# Config
# ==========================
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
PREFIX=${PREFIX:-p1}
KADMIN_BIN=${KADMIN_BIN:-./bin/kadmin}

ORDERS_PARTITIONS=${ORDERS_PARTITIONS:-4}
ENRICHED_PARTITIONS=${ENRICHED_PARTITIONS:-4}
OUTPUT_PARTITIONS=${OUTPUT_PARTITIONS:-4}

TOPIC_RAW=${PREFIX}.orders
TOPIC_IN=${PREFIX}.orders.enriched
TOPIC_OUT=${PREFIX}.orders.output
TOPIC_CHANGELOG=${PREFIX}.opb-changelog
TOPIC_SNAPSHOTS=${PREFIX}.opb-snapshots

say() { printf "[INFRA] [%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

require_kadmin() {
  if [ ! -x "$KADMIN_BIN" ]; then
    say "Building kadmin (Kafka Go admin helper)..."
    go build -o "$KADMIN_BIN" ./cmd/kadmin
  fi
}

kadmin() {
  "$KADMIN_BIN" -bootstrap "$BOOTSTRAP" "$@"
}

# Helpers
topic_exists() {
  local t=$1
  local out
  out=$(kadmin -cmd describe -topic "$t" 2>/dev/null || true)
  [[ "$out" == *"exists=true"* ]]
}

wait_for_kafka() {
  say "Waiting for Kafka at ${BOOTSTRAP}..."
  for i in {1..60}; do
      if kadmin -cmd describe -topic "__consumer_offsets" >/dev/null 2>&1; then
          say "Kafka is ready."
          return 0
      fi
      sleep 1
  done
  say "ERROR: Timed out waiting for Kafka."
  exit 1
}

wait_deleted() {
  local t=$1 timeout=${2:-60}
  for i in $(seq 1 "$timeout"); do
    if ! topic_exists "$t"; then return 0; fi
    sleep 1
  done
  return 1
}

create_if_missing() {
  local t=$1 parts=$2 rf=$3 extra=${4:-}
  if topic_exists "$t"; then
    say "Topic exists, skip create: ${t}"
  else
    say "Creating topic ${t} (partitions=${parts}, rf=${rf})"
    if [[ -n "$extra" ]]; then
      kadmin -cmd create -topic "$t" -partitions "$parts" -rf "$rf" -config "$extra"
    else
      kadmin -cmd create -topic "$t" -partitions "$parts" -rf "$rf"
    fi
  fi
}

delete_topics() {
  say "Deleting old topics (prefix=${PREFIX})..."
  local topics=("${TOPIC_RAW}" "${TOPIC_IN}" "${TOPIC_OUT}" "${TOPIC_CHANGELOG}" "${TOPIC_SNAPSHOTS}")
  for topic in "${topics[@]}"; do
    if topic_exists "$topic"; then
      say "Deleting topic ${topic}..."
      kadmin -cmd delete -topic "${topic}" || true
    fi
  done
  say "Waiting for topics to be fully deleted..."
  local failed=0
  for topic in "${topics[@]}"; do
    if wait_deleted "$topic" 60; then
      say "Deleted: ${topic}"
    else
      say "WARN: Topic still present after timeout: ${topic}"
      failed=1
    fi
  done
  if [[ "$failed" -eq 1 ]]; then
    say "WARN: Some topics may still exist, proceeding will attempt idempotent creates."
  fi
}

ensure_topics() {
  say "Creating topics (prefix=${PREFIX})..."
  create_if_missing "${TOPIC_RAW}" "$ORDERS_PARTITIONS" 1
  create_if_missing "${TOPIC_IN}" "$ENRICHED_PARTITIONS" 1
  create_if_missing "${TOPIC_OUT}" "$OUTPUT_PARTITIONS" 1
  create_if_missing "${TOPIC_CHANGELOG}" 1 1 "cleanup.policy=compact"
  create_if_missing "${TOPIC_SNAPSHOTS}" 1 1 "cleanup.policy=compact"
  say "All topics are ready."
}

main() {
  require_kadmin
  wait_for_kafka
  delete_topics
  ensure_topics
}

main "$@"
