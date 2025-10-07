BINARY=bin/opb
GENERATOR=bin/genorders
PKG_OPB=./cmd/opb
PKG_GEN=./cmd/genorders

.PHONY: build run clean gen obs-start obs-stop

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

# Observability: start Prometheus (Grafana can point to it)
obs-start:
	@echo "Starting Prometheus with ./prometheus.yml on :9095";
	@pkill -f "prometheus --config.file=./prometheus.yml" || true;
	@nohup prometheus --config.file=./prometheus.yml --web.listen-address=":9095" >/tmp/prometheus.out 2>&1 &
	@echo "Prometheus started at http://localhost:9095"

obs-stop:
	@echo "Stopping Prometheus";
	@pkill -f "prometheus --config.file=./prometheus.yml" || true;
	@echo "Stopped"
