#!/usr/bin/env bash
set -euo pipefail

# High-load benchmark: generate synthetic input at high RPS with parallel producers,
# sample Kafka group lag and OpB metrics to observe backpressure and throughput.

# When using Apache Kafka (Java), set KAFKA_NATIVE=true and BROKERS_HOST=127.0.0.1:9092
BROKERS_HOST=${BROKERS_HOST:-localhost:19092}
BROKERS_DOCKER=${BROKERS_DOCKER:-redpanda:9092}
TOPIC_IN=${TOPIC_IN:-p2.orders.enriched}
GROUP=${GROUP:-opb-g1}
USE_LOCAL_RPK=${USE_LOCAL_RPK:-false}   # true => use local rpk; false => docker exec redpanda rpk
DRY_RUN=${DRY_RUN:-false}               # true => print what would run, don't send
PARTITIONS=${PARTITIONS:-6}             # topic partitions to create if missing

# Concurrency params
DURATION=${DURATION:-60}           # seconds
PROCS=${PROCS:-50}                # parallel producers (approximate concurrency)
RPS_PER_PROC=${RPS_PER_PROC:-200} # messages per second per process

# Resolve producer/admin commands
if [[ "${KAFKA_NATIVE:-false}" == "true" ]]; then
  # Apache Kafka CLI
  KAFKA_TOPICS="kafka-topics --bootstrap-server $BROKERS_HOST"
  KAFKA_PRODUCER="kafka-console-producer --bootstrap-server $BROKERS_HOST --topic $TOPIC_IN"
  RPK=""
else
  # Redpanda rpk
  if [[ "$USE_LOCAL_RPK" == "true" ]]; then
    RPK="rpk --brokers $BROKERS_HOST"
  else
    RPK="docker exec -i redpanda rpk --brokers $BROKERS_DOCKER"
  fi
fi

TOTAL=$((PROCS * RPS_PER_PROC * DURATION))
echo "== load-benchmark: duration=${DURATION}s procs=${PROCS} rps_per_proc=${RPS_PER_PROC} total~${TOTAL} use_local_rpk=${USE_LOCAL_RPK} dry_run=${DRY_RUN} =="

maybe_create_topic() {
  # Create topic if missing (best-effort)
  if [[ "$DRY_RUN" == "true" ]]; then
    if [[ "${KAFKA_NATIVE:-false}" == "true" ]]; then
      echo "[DRY] $KAFKA_TOPICS --create --topic $TOPIC_IN --partitions $PARTITIONS --replication-factor 1 || true"
    else
      echo "[DRY] $RPK topic create $TOPIC_IN -p $PARTITIONS -r 1 || true"
    fi
  else
    if [[ "${KAFKA_NATIVE:-false}" == "true" ]]; then
      bash -c "$KAFKA_TOPICS --create --topic $TOPIC_IN --partitions $PARTITIONS --replication-factor 1" >/dev/null 2>&1 || true
    else
      bash -c "$RPK topic create $TOPIC_IN -p $PARTITIONS -r 1" >/dev/null 2>&1 || true
    fi
  fi
}

produce_proc() {
  local seed="$1"
  if [[ "$DRY_RUN" == "true" ]]; then
    if [[ "${KAFKA_NATIVE:-false}" == "true" ]]; then
      echo "[DRY] python3 - <pygen | $KAFKA_PRODUCER >/dev/null"
    else
      echo "[DRY] python3 - <pygen | $RPK topic produce $TOPIC_IN >/dev/null"
    fi
    return 0
  fi
  if [[ "${KAFKA_NATIVE:-false}" == "true" ]]; then
    python3 - "$seed" "$DURATION" "$RPS_PER_PROC" <<'PY' | bash -c "$KAFKA_PRODUCER" >/dev/null
import sys, time, json, random
seed = int(sys.argv[1])
duration = int(sys.argv[2])
rps = int(sys.argv[3])
random.seed(seed)
end = time.time() + duration
sleep = 1 / rps if rps > 0 else 0.001
stores = ['A','B','C','D']
i = 0
while time.time() < end:
  ts = int(time.time())
  payload = {"orderId": f"o-bench-{seed}-{i}", "productId": f"p{random.randint(1,999999)}", "storeId": random.choice(stores), "price": 10000, "qty": 1, "ts": ts, "validated": True, "normTs": ts}
  sys.stdout.write(json.dumps(payload)+"\n"); sys.stdout.flush(); i += 1
  time.sleep(sleep)
PY
  else
    python3 - "$seed" "$DURATION" "$RPS_PER_PROC" <<'PY' | bash -c "$RPK topic produce $TOPIC_IN" >/dev/null
import sys, time, json, random
seed = int(sys.argv[1])
duration = int(sys.argv[2])
rps = int(sys.argv[3])
random.seed(seed)
end = time.time() + duration
sleep = 1 / rps if rps > 0 else 0.001
stores = ['A','B','C','D']
i = 0
while time.time() < end:
  ts = int(time.time())
  payload = {"orderId": f"o-bench-{seed}-{i}", "productId": f"p{random.randint(1,999999)}", "storeId": random.choice(stores), "price": 10000, "qty": 1, "ts": ts, "validated": True, "normTs": ts}
  sys.stdout.write(json.dumps(payload)+"\n"); sys.stdout.flush(); i += 1
  time.sleep(sleep)
PY
  fi
}

sample_metrics() {
  echo "-- metrics snapshot --"
  if [[ "$DRY_RUN" == "true" ]]; then
    if [[ "${KAFKA_NATIVE:-false}" == "true" ]]; then
      echo "[DRY] kafka-consumer-groups --bootstrap-server $BROKERS_HOST --describe --group $GROUP | head -n 120"
    else
      echo "[DRY] $RPK group describe $GROUP | head -n 120"
    fi
  else
    if [[ "${KAFKA_NATIVE:-false}" == "true" ]]; then
      bash -c "kafka-consumer-groups --bootstrap-server $BROKERS_HOST --describe --group $GROUP" | sed -n '1,120p' || true
    else
      bash -c "$RPK group describe $GROUP" | sed -n '1,120p' || true
    fi
  fi
  # OpB metrics (first instance assumed :8082)
  curl -s localhost:8082/metrics | egrep -e '^opb_partition_lag' -e '^opb_tx_.*' -e '^opb_changelog_.*' | sed -n '1,120p' || true
}

trap 'echo "Interrupted"; exit 1' INT TERM

maybe_create_topic

# Launch producers
for p in $(seq 1 "$PROCS"); do produce_proc "$p" & done

# Periodically sample (every 10s)
if (( DURATION >= 10 )); then
  for t in $(seq 1 $((DURATION/10))); do
    sleep 10
    sample_metrics
  done
fi

wait
echo "== load-benchmark: done =="


