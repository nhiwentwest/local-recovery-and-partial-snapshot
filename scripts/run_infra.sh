#!/usr/bin/env bash
set -euo pipefail
# [CỬA SỔ 1] - INFRASTRUCTURE
# Nhiệm vụ: Xóa và tạo lại các topic Kafka để đảm bảo môi trường sạch.

# ==========================
# Config
# ==========================
BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
PREFIX=${PREFIX:-p2}
KAFKA_TOPICS=${KAFKA_TOPICS:-/opt/homebrew/bin/kafka-topics}

TOPIC_RAW=${PREFIX}.orders
TOPIC_IN=${PREFIX}.orders.enriched
TOPIC_OUT=${PREFIX}.orders.output
TOPIC_CHANGELOG=${PREFIX}.opb-changelog
TOPIC_SNAPSHOTS=${PREFIX}.opb-snapshots

say() { printf "[INFRA] [%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

# Chờ Kafka sẵn sàng
wait_for_kafka() {
  say "Waiting for Kafka at ${BOOTSTRAP}..."
  for i in {1..60}; do
      if ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
          say "Kafka is ready."
          return 0
      fi
      sleep 1
  done
  say "ERROR: Timed out waiting for Kafka."
  exit 1
}

delete_topics() {
  say "Deleting old topics (prefix=${PREFIX})..."
  local topics=("${TOPIC_RAW}" "${TOPIC_IN}" "${TOPIC_OUT}" "${TOPIC_CHANGELOG}" "${TOPIC_SNAPSHOTS}")
  for topic in "${topics[@]}"; do
    if ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --describe --topic "${topic}" >/dev/null 2>&1; then
      say "Deleting topic ${topic}..."
      ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --delete --topic "${topic}"
    fi
  done
  say "Finished deleting topics. Waiting a moment for changes to propagate..."
  sleep 3
}

ensure_topics() {
  say "Creating topics (prefix=${PREFIX})..."
  ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --create --topic "${TOPIC_RAW}" --partitions 6 --replication-factor 1
  ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --create --topic "${TOPIC_IN}" --partitions 6 --replication-factor 1
  ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --create --topic "${TOPIC_OUT}" --partitions 6 --replication-factor 1
  ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --create --topic "${TOPIC_CHANGELOG}" --partitions 1 --replication-factor 1 --config cleanup.policy=compact
  ${KAFKA_TOPICS} --bootstrap-server "${BOOTSTRAP}" --create --topic "${TOPIC_SNAPSHOTS}" --partitions 1 --replication-factor 1 --config cleanup.policy=compact
  say "All topics are ready."
}

main() {
  wait_for_kafka
  delete_topics
  ensure_topics
}

main "$@"