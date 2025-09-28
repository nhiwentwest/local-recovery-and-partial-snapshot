#!/usr/bin/env bash
set -euo pipefail

# Bounded 60s end-to-end benchmark using Docker Redpanda and local rpk
# Requirements: Docker, docker-compose, rpk (brew), Go toolchain

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

BROKER_HOST="localhost:19092"
GROUP_ID="opb-g-60s"
TOPIC_IN="p2.orders.enriched.60s"
TOPIC_OUT="orders.output.60s"
PARTS_IN=60
PARTS_OUT=24
DURATION=60
PROCS=50
RPS_PER_PROC=200

cleanup() {
  set +e
  pkill -f "bin/opb" >/dev/null 2>&1 || true
  docker-compose down >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 1) Start broker container
echo "== Starting Redpanda container =="
docker-compose up -d redpanda
sleep 6

# 2) Build opb
echo "== Building opb =="
go build -o ./bin/opb ./cmd/opb

# 3) Create topics
echo "== Creating topics ${TOPIC_IN}(${PARTS_IN}) and ${TOPIC_OUT}(${PARTS_OUT}) =="
rpk topic create "${TOPIC_IN}" --partitions=${PARTS_IN} --replicas=1 >/dev/null 2>&1 || true
rpk topic create "${TOPIC_OUT}" --partitions=${PARTS_OUT} --replicas=1 >/dev/null 2>&1 || true

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
rpk group describe ${GROUP_ID} || true
rpk topic describe ${TOPIC_IN} || true

# 9) Tear down container
docker-compose down

echo "== Done (bounded run with full cleanup) =="
