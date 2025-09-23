# OpA (stateless normalize, EOS KIP-98)

## Run (local dev)

1. Start Kafka (Redpanda in repo docker-compose)
```
docker compose up -d
```

2. Create topics (p1.*)
```
./scripts/opa-topics.sh
```

3. Build & run OpA
```
go build -o bin/opa ./cmd/opa
./bin/opa --bootstrap localhost:19092 --group-id opa \
  --topic-in p1.orders --topic-out p1.orders.enriched --tx-id opa-local-1
```

4. Crash matrix (optional)
```
./bin/opa --bootstrap localhost:19092 --group-id opa \
  --topic-in p1.orders --topic-out p1.orders.enriched --tx-id opa-local-1 \
  --crash-mode before|mid|after
```

## Notes
- Consumer: enable.auto.commit=false, isolation.level=read_committed
- Producer: enable.idempotence=true, transactional.id=<tx>
- Flow: InitTransactions → Begin → Normalize → Produce → SendOffsetsToTransaction → Commit
- On error: AbortTransaction

## Schema
- See contracts/schema-v1.md

