BINARY=bin/opb
GENERATOR=bin/genorders
PKG_OPB=./cmd/opb
PKG_GEN=./cmd/genorders

.PHONY: build run clean gen

build:
	mkdir -p bin
	GO111MODULE=on go build -o $(BINARY) $(PKG_OPB)
	GO111MODULE=on go build -o $(GENERATOR) $(PKG_GEN)

run: build
	./$(BINARY) --topic-prefix p2 --snapshot-dir ./snapshots --badger-dir ./data/opb

gen: build
	./$(GENERATOR) -count 50 -output p2.orders.enriched.jsonl

clean:
	rm -rf bin
