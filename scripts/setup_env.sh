#!/usr/bin/env bash
# This script is meant to be sourced by other demo scripts to ensure a clean and ready environment.

set -euo pipefail

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}

say() { printf "\n\e[1;36m[SETUP]\e[0m %s\n" "$*"; }

# --- Helper Functions ---

ensure_kafka_cli() {
  KAFKA_TOPICS_CMD=""
  if command -v kafka-topics >/dev/null 2>&1; then
    KAFKA_TOPICS_CMD="kafka-topics"
  elif [ -x /opt/homebrew/bin/kafka-topics ]; then
    KAFKA_TOPICS_CMD="/opt/homebrew/bin/kafka-topics"
  fi
  if [ -z "$KAFKA_TOPICS_CMD" ]; then
    echo "ERROR: kafka-topics command not found." >&2
    return 1
  fi

  KAFKA_GROUPS_CMD=""
  if command -v kafka-consumer-groups >/dev/null 2>&1; then
    KAFKA_GROUPS_CMD="kafka-consumer-groups"
  elif [ -x /opt/homebrew/bin/kafka-consumer-groups ]; then
    KAFKA_GROUPS_CMD="/opt/homebrew/bin/kafka-consumer-groups"
  fi
  if [ -z "$KAFKA_GROUPS_CMD" ]; then
    echo "ERROR: kafka-consumer-groups command not found." >&2
    return 1
  fi
}

create_topic() {
  local topic=$1; local parts=$2; local extra_config=${3:-}
  say "Ensuring topic '$topic' exists with $parts partitions..."
  "$KAFKA_TOPICS_CMD" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$topic" --partitions "$parts" --replication-factor 1 $extra_config >/dev/null 2>&1 || true
}

wait_topic_ready() {
  local topic=$1; local attempts=${2:-15}
  printf "Waiting for topic '$topic' to be ready..."
  for ((i=0;i<attempts;i++)); do
    if "$KAFKA_TOPICS_CMD" --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic" >/dev/null 2>&1; then
      echo " OK"
      return 0
    fi
    sleep 1
    printf "."
  done
  echo " ERROR: Timeout waiting for topic '$topic' to be ready." >&2
  return 1
}

# --- Main Setup Logic ---

ensure_kafka_cli

say "Cleaning up previous run..."
pkill -f "./bin/opb" || true
say "Waiting for consumers to leave group (12s)..."
sleep 12
"$KAFKA_GROUPS_CMD" --bootstrap-server "$BOOTSTRAP" --delete --group opb-standalone --timeout 5000 || true
sleep 2

say "Preparing Kafka topics..."
create_topic p1.orders.enriched 4
create_topic p1.orders.output 4
create_topic p1.opb-audit 1
create_topic p1.opb-snapshots 3 "--config cleanup.policy=compact"
create_topic p1.opb-changelog 3 "--config cleanup.policy=compact"
create_topic p1.opb-store-touch 3 "--config cleanup.policy=compact"

say "Waiting for all topics to be ready..."
wait_topic_ready p1.orders.enriched
wait_topic_ready p1.opb-snapshots
wait_topic_ready p1.opb-changelog
wait_topic_ready p1.opb-store-touch

say "Environment is clean and ready."
