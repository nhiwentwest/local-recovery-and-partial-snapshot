#!/usr/bin/env bash
set -euo pipefail

# Kiểm tra phân phối partition cho key enriched (A#p{pid}#windowStart)
# Dùng Murmur2 giống Kafka để tính partition, in ra thống kê counts/partition
# Cách dùng: TOPIC=p2.orders.output KEYPREFIX=A PIDS=100 ./scripts/check_spread.sh

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
TOPIC=${TOPIC:-p2.orders.output}
KEYPREFIX=${KEYPREFIX:-A}
PIDS=${PIDS:-100}

say(){ printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

parts() {
  kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC" 2>/dev/null \
    | awk -F"PartitionCount:" '/PartitionCount/ {print $2}' | awk '{print $1}'
}

main(){
  local cnt; cnt=$(parts); if [[ -z "$cnt" ]]; then cnt=1; fi
  say "topic=$TOPIC partitions=$cnt pid_range=$PIDS keyprefix=$KEYPREFIX"
  declare -a buckets; for ((i=0;i<cnt;i++)); do buckets[$i]=0; done
  local win=$(( ($(date +%s)/10)*10 ))
  for ((p=1;p<=PIDS;p++)); do
    key="${KEYPREFIX}#p${p}#${win}"
    part=$(go run ./cmd/hash_murmur2 "$key" "$cnt" 2>/dev/null)
    buckets[$part]=$(( buckets[$part] + 1 ))
  done
  for ((i=0;i<cnt;i++)); do echo "partition[$i]="${buckets[$i]}""; done
}

main "$@"


