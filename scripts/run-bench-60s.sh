#!/usr/bin/env bash
set -euo pipefail

# Bounded 60s end-to-end benchmark using native Redpanda and local rpk
# Requirements: redpanda (brew), rpk (brew), Go toolchain

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

BROKER_HOST="127.0.0.1:9092"
GROUP_ID="opb-g-60s"
TOPIC_IN="p2.orders.enriched.60s"
TOPIC_OUT="orders.output.60s"
PARTS_IN=${PARTS_IN:-60}
PARTS_OUT=${PARTS_OUT:-24}
DURATION=${DURATION:-60}
PROCS=${PROCS:-50}
RPS_PER_PROC=${RPS_PER_PROC:-200}

cleanup() {
  set +e
  pkill -f "bin/opb" >/dev/null 2>&1 || true
}
trap cleanup EXIT

ensure_redpanda() {
  # Start native Redpanda if not listening on 127.0.0.1:9092
  if ! nc -z 127.0.0.1 9092 >/dev/null 2>&1; then
    echo "== Starting Redpanda (native) =="
    # Try with redpanda if available; fallback to rpk redpanda start
    if command -v redpanda >/dev/null 2>&1; then
      redpanda start --overprovisioned --smp 6 --memory 6G \
        --reserve-memory 0M --check=false \
        --kafka-addr 0.0.0.0:9092 \
        --advertise-kafka-addr 127.0.0.1:9092 \
        --data-directory ./data/redpanda \
        --default-log-level=warn --wait-for-leader >/dev/null 2>&1 &
    else
      rpk redpanda start --smp=6 --memory=6144M \
        --advertise-kafka-addr=PLAINTEXT://127.0.0.1:9092 \
        --default-log-level=warn >/dev/null 2>&1 &
    fi
    # wait for broker
    for i in $(seq 1 30); do
      sleep 1
      if nc -z 127.0.0.1 9092 >/dev/null 2>&1; then break; fi
    done
    if ! nc -z 127.0.0.1 9092 >/dev/null 2>&1; then
      echo "Redpanda failed to start on 127.0.0.1:9092" >&2
      exit 1
    fi
  else
    echo "== Redpanda already running on 127.0.0.1:9092 =="
  fi
}

# 1) Ensure native broker is running
ensure_redpanda

# 2) Build opb
echo "== Building opb =="
go build -o ./bin/opb ./cmd/opb

# 3) Create topics (native)
echo "== Creating topics ${TOPIC_IN}(${PARTS_IN}) and ${TOPIC_OUT}(${PARTS_OUT}) =="
rpk --brokers ${BROKER_HOST} topic create "${TOPIC_IN}" --partitions=${PARTS_IN} --replicas=1 >/dev/null 2>&1 || true
rpk --brokers ${BROKER_HOST} topic create "${TOPIC_OUT}" --partitions=${PARTS_OUT} --replicas=1 >/dev/null 2>&1 || true

# 4) Clear previous local pebble data
rm -rf ./data/opb-pebble-* || true

# 5) Start 6 opb replicas (EOS)
echo "== Starting 6 opb replicas =="
./bin/opb --input-source=kafka --kafka-bootstrap=${BROKER_HOST} --topic-enriched=${TOPIC_IN} --group-id=${GROUP_ID} --state-backend=pebble --state-dir=./data/opb-pebble-1 --http=:8083 --instance-id=opb1 --output-tx-id=opb-tx-1 --output-topic=${TOPIC_OUT} &
sleep 0.5; ./bin/opb --input-source=kafka --kafka-bootstrap=${BROKER_HOST} --topic-enriched=${TOPIC_IN} --group-id=${GROUP_ID} --state-backend=pebble --state-dir=./data/opb-pebble-2 --http=:8084 --instance-id=opb2 --output-tx-id=opb-tx-2 --output-topic=${TOPIC_OUT} &
sleep 0.5; ./bin/opb --input-source=kafka --kafka-bootstrap=${BROKER_HOST} --topic-enriched=${TOPIC_IN} --group-id=${GROUP_ID} --state-backend=pebble --state-dir=./data/opb-pebble-3 --http=:8085 --instance-id=opb3 --output-tx-id=opb-tx-3 --output-topic=${TOPIC_OUT} &
sleep 0.5; ./bin/opb --input-source=kafka --kafka-bootstrap=${BROKER_HOST} --topic-enriched=${TOPIC_IN} --group-id=${GROUP_ID} --state-backend=pebble --state-dir=./data/opb-pebble-4 --http=:8086 --instance-id=opb4 --output-tx-id=opb-tx-4 --output-topic=${TOPIC_OUT} &
sleep 0.5; ./bin/opb --input-source=kafka --kafka-bootstrap=${BROKER_HOST} --topic-enriched=${TOPIC_IN} --group-id=${GROUP_ID} --state-backend=pebble --state-dir=./data/opb-pebble-5 --http=:8087 --instance-id=opb5 --output-tx-id=opb-tx-5 --output-topic=${TOPIC_OUT} &
sleep 0.5; ./bin/opb --input-source=kafka --kafka-bootstrap=${BROKER_HOST} --topic-enriched=${TOPIC_IN} --group-id=${GROUP_ID} --state-backend=pebble --state-dir=./data/opb-pebble-6 --http=:8088 --instance-id=opb6 --output-tx-id=opb-tx-6 --output-topic=${TOPIC_OUT} &

# 6) Run 60s benchmark (local rpk)
echo "== Running benchmark: ${DURATION}s, procs=${PROCS}, rps_per_proc=${RPS_PER_PROC} =="
USE_LOCAL_RPK=true BROKERS_HOST=${BROKER_HOST} TOPIC_IN=${TOPIC_IN} GROUP=${GROUP_ID} DURATION=${DURATION} PROCS=${PROCS} RPS_PER_PROC=${RPS_PER_PROC} ./scripts/load-benchmark.sh

# 7) Stop opb
pkill -f "bin/opb" || true

# 8) Print summary
echo "== Summary =="
rpk --brokers ${BROKER_HOST} group describe ${GROUP_ID} || true
rpk --brokers ${BROKER_HOST} topic describe ${TOPIC_IN} || true

echo "== Done (bounded run native) =="
