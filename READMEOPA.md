OpA — exactly-once normalizer
Normalize and enrich raw ride orders. Produce deduped records to orders.norm using Kafka transactions (KIP-98). Includes opa_ordergen for local load.
Repository
Name: opa
Primary module: github.com/<you>/opa
opa/
├─ README.md
├─ docker-compose.yml            # Kafka/Redpanda for local dev
├─ Makefile
├─ contracts/
│  └─ schema-v1.md              # raw and norm event schemas
├─ scripts/
│  └─ kafka/
│     ├─ create-topics.sh
│     ├─ consume-norm.sh
│     └─ produce-sample.sh
├─ cmd/
│  ├─ opa/                      # service: consumes raw, produces norm with EOS
│  │  └─ main.go
│  └─ opa_ordergen/             # simple order generator for testing
│     └─ main.go
└─ internal/
   ├─ eos/                      # EOS helpers: tx producer, send offsets in tx
   └─ model/                    # domain types, serde, normalization
What OpA does
Consume orders.raw.
Validate and normalize. Enrich with areaId and minute bucket.
Produce to orders.norm keyed by stable orderId.
Commit consumer offsets in the same transaction.

File types and purpose
Root
README.md
Type: Markdown.
Use: Project docs. Copy these instructions here.
docker-compose.yml
Type: YAML.
Use: Local Kafka/Redpanda stack for dev and tests.
Makefile
Type: Make.
Use: Shortcuts for up, down, topics, build, run-opa, gen.
contracts/
schema-v1.md
Type: Markdown spec.
Use: Defines orders.raw and orders.norm event shapes and keys.
scripts/kafka/
create-topics.sh
Type: Bash.
Use: Create orders.raw and orders.norm with correct configs.
consume-norm.sh
Type: Bash.
Use: Tail orders.norm for smoke checks.
produce-sample.sh (optional)
Type: Bash.
Use: Send a few raw messages for manual tests.
cmd/opa/
main.go
Type: Go entrypoint.
Use: OpA service. Consumes orders.raw, normalizes, enriches, produces orders.norm. Commits offsets in the same transaction.
cmd/opa_ordergen/
main.go
Type: Go entrypoint.
Use: Load generator for orders.raw. Flags like --rate, --count, --brokers, --topic.
internal/eos/ (library code used by OpA)
producer_tx.go
Type: Go.
Use: Idempotent + transactional producer wrapper (begin/commit/abort).
consumer_group.go
Type: Go.
Use: Group consumer setup and polling.
offsets_tx.go
Type: Go.
Use: SendOffsetsToTransaction helper bound to group metadata.
retry_backoff.go
Type: Go.
Use: Bounded retries with jitter for tx begin/commit.
internal/model/ (domain and transforms)
order_raw.go
Type: Go.
Use: Raw order struct, validation.
order_norm.go
Type: Go.
Use: Normalized order struct, stable keys.
normalize.go
Type: Go.
Use: Shape fixes, field coercion, defaults.
enrich.go
Type: Go.
Use: Derive areaId, minute bucket, fare rounding.
serde.go
Type: Go.
Use: JSON/Avro encoding, headers, key serde.
partitioning.go
Type: Go.
Use: Key strategy and partition hints.
build outputs
bin/opa, bin/opa_ordergen
Type: Binaries.
Use: Built artifacts from make build-opa and make build-gen.

Prerequisites
Go ≥ 1.22
Docker and Docker Compose v2
Bash
Quick start
# scaffold (if starting fresh)
mkdir -p cmd/opa cmd/opa_ordergen internal/{eos,model} scripts/kafka contracts

# init module
go mod init github.com/<you>/opa
go mod tidy
Kafka stack (docker)
docker-compose.yml (minimal Redpanda)
version: "3.8"
services:
  redpanda:
    image: docker.redpanda.com/redpanda/redpanda:v24.1.8
    command:
      - redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M
        --node-id 0 --check=false --kafka-addr PLAINTEXT://0.0.0.0:9092
        --advertise-kafka-addr PLAINTEXT://localhost:9092
    ports: [ "9092:9092", "9644:9644" ]
Bring up:
docker compose up -d
Topics
scripts/kafka/create-topics.sh
#!/usr/bin/env bash
set -euo pipefail
rpk cluster info >/dev/null 2>&1 || { echo "Install rpk: https://docs.redpanda.com"; exit 1; }

# raw: delete policy, short retention
rpk topic create orders.raw \
  --partitions 6 --replicas 1 \
  --config retention.ms=$((2*24*60*60*1000)) \
  --config cleanup.policy=delete

# norm: compacted keyed by stable orderId
rpk topic create orders.norm \
  --partitions 6 --replicas 1 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1 \
  --config segment.ms=600000
Create them:
bash scripts/kafka/create-topics.sh
Configuration
Environment variables:
KAFKA_BROKERS=localhost:9092
OPA_GROUP=opa-g1
OPA_TXN_ID=opa-txn-1
TOPIC_RAW=orders.raw
TOPIC_NORM=orders.norm
# enrichment
GRID_SIZE_M=500
TZ_OFFSET_MIN=0
Build and run
With Go:
# service
go run ./cmd/opa

# generator
go run ./cmd/opa_ordergen --rate 200 --count 5000 \
  --topic ${TOPIC_RAW:-orders.raw} --brokers ${KAFKA_BROKERS:-localhost:9092}
With Make:
make up           # docker compose up -d
make topics       # create topics
make run-opa      # go run ./cmd/opa
make gen          # go run ./cmd/opa_ordergen ...
Suggested Makefile targets:
up: ; docker compose up -d
down: ; docker compose down -v
topics: ; bash scripts/kafka/create-topics.sh
build-opa: ; go build -o bin/opa ./cmd/opa
build-gen: ; go build -o bin/opa_ordergen ./cmd/opa_ordergen
run-opa: ; KAFKA_BROKERS?=localhost:9092 go run ./cmd/opa
gen: ; go run ./cmd/opa_ordergen --rate 100 --count 10000
Smoke test
Start infra and topics.
docker compose up -d
bash scripts/kafka/create-topics.sh
Run OpA.
KAFKA_BROKERS=localhost:9092 OPA_GROUP=opa-g1 OPA_TXN_ID=opa-txn-1 \
go run ./cmd/opa
Generate traffic.
go run ./cmd/opa_ordergen --rate 300 --count 3000
Inspect output.
Using rpk:
# tail normalized stream
rpk topic consume orders.norm -n 10

# check lag
rpk group describe opa-g1
Optional consumer script:
scripts/kafka/consume-norm.sh
#!/usr/bin/env bash
rpk topic consume orders.norm -f '%k | %v' -n 20
Event contracts (v1)
orders.raw (value): minimal producer payload. Must include source, sourceOrderId, ts, lat, lon.
orders.norm (key=value):
key: orderId = <source>:<sourceOrderId>
value: normalized order with orderId, tsMinute, areaId, fare, status, etc.
See contracts/schema-v1.md.
Exactly-once behavior
Idempotent producer enabled.
Transaction wraps produced records and offset commits.
On crash before commit: transaction aborts. Offsets not advanced. Records reprocessed. Duplicates suppressed by compaction on key.
Log codes (JSON):
OPA_VALIDATE_FAIL, OPA_NORMALIZE_OK, OPA_ENRICH_OK,
OPA_TX_BEGIN, OPA_TX_SEND_OFFSETS, OPA_TX_COMMIT_OK, OPA_TX_ABORT.
Testing notes
Unit: normalization, key derivation, enrichment.
Integration: transaction commit/abort paths with embedded broker or Redpanda.
Chaos: kill OpA mid-batch, restart, verify no double count in orders.norm.
Troubleshooting
GROUP_AUTHORIZATION_FAILED: group name conflict in managed clusters. Change OPA_GROUP.
INVALID_PRODUCER_EPOCH: reuse of OPA_TXN_ID after unclean shutdown. Delete producer ID on broker or change OPA_TXN_ID.
No output: check topic names and keys; confirm orders.raw receive events.