SHELL := powershell.exe
.SHELLFLAGS := -NoProfile -Command
BINARY=bin/opb.exe
GENERATOR=bin/genorders.exe
PKG_OPB=./cmd/opb
PKG_GEN=./cmd/genorders

.PHONY: build run clean gen

build:
	if (!(Test-Path bin)) { New-Item -ItemType Directory -Path bin }
	$$env:GOOS="windows"; $$env:GOARCH="amd64"; $$env:GO111MODULE="on"; go build -o $(BINARY) $(PKG_OPB)
	$$env:GOOS="windows"; $$env:GOARCH="amd64"; $$env:GO111MODULE="on"; go build -o $(GENERATOR) $(PKG_GEN)

run: build
	.\$(BINARY) --topic-prefix p2 --snapshot-dir ./snapshots --badger-dir ./data/opb

gen: build
	.\$(GENERATOR) --count 50 --output p2.orders.enriched.jsonl

clean:
	Remove-Item -Path bin -Recurse -Force

# Person 3 - Recovery and Metrics (Safe isolated testing)
P3_RECOVERY=bin/p3-recovery.exe
P3_DATAGEN=bin/p3-datagen.exe
P3_FAILURE_INJECTOR=bin/p3-failure-injector.exe

p3-build:
	if (!(Test-Path bin)) { New-Item -ItemType Directory -Path bin }
	$$env:GOOS="windows"; $$env:GOARCH="amd64"; $$env:GO111MODULE="on"; go build -o $(P3_DATAGEN) ./cmd/p3-datagen
	$$env:GOOS="windows"; $$env:GOARCH="amd64"; $$env:GO111MODULE="on"; go build -o $(P3_RECOVERY) ./cmd/recovery-standalone
	$$env:GOOS="windows"; $$env:GOARCH="amd64"; $$env:GO111MODULE="on"; go build -o $(P3_FAILURE_INJECTOR) ./cmd/p3-failure-injector

p3-setup: p3-build
	.\$(P3_DATAGEN)

p3-recovery: p3-build
	.\$(P3_RECOVERY) -snapshot-dir .

# THÊM CÁC TARGET KAFKA MỚI
p3-recovery-kafka: p3-build
	.\$(P3_RECOVERY) --use-kafka --bootstrap localhost:9092 --topic-manifest p2.opb-snapshots --topic-changelog p2.opb-changelog

p3-recovery-kafka-dev: p3-build
	.\$(P3_RECOVERY) --use-kafka --bootstrap localhost:19092 --topic-manifest p2.opb-snapshots --topic-changelog p2.opb-changelog

p3-failure-test: p3-build
	.\$(P3_FAILURE_INJECTOR)

p3-metrics:
	Invoke-WebRequest -Uri http://localhost:2112/metrics | Select-String "opb_recovery"

p3-metrics-all:
	Invoke-WebRequest -Uri http://localhost:2112/metrics | Select-String "opb_"

# CẬP NHẬT p3-clean-data ĐỂ XÓA CÁC FILE MỚI
p3-clean-data:
	-Remove-Item -Path "p3-snapshot-*" -Recurse -Force -ErrorAction SilentlyContinue
	-Remove-Item -Path "manifest.latest.json" -Force -ErrorAction SilentlyContinue
	-Remove-Item -Path "manifest-p3.json" -Force -ErrorAction SilentlyContinue
	-Remove-Item -Path "snapshots-integrated" -Recurse -Force -ErrorAction SilentlyContinue
	-Remove-Item -Path "changelog" -Recurse -Force -ErrorAction SilentlyContinue

p3-recovery-integrated: p3-build
	@echo "=== Using ONLY integrated snapshot ==="
	Remove-Item -Path "p3-snapshot-*" -Recurse -Force -ErrorAction SilentlyContinue
	.\$(P3_RECOVERY) -snapshot-dir .

# THÊM TARGET TEST ALL
p3-test-all: p3-clean-data p3-setup p3-failure-test

# THÊM TARGET INTEGRATION TEST
p3-integration-test: p3-clean-data p3-setup
	@echo "=== Integration Test: Running recovery with integrated snapshots ==="
	.\$(P3_RECOVERY) -snapshot-dir . -poll 10

p3-help:
	@echo "Person 3 Targets:"
	@echo "  p3-setup              - Generate test data"
	@echo "  p3-recovery           - Run recovery service (file mode)" 
	@echo "  p3-recovery-kafka     - Run recovery service (Kafka mode - port 9092)"
	@echo "  p3-recovery-kafka-dev - Run recovery service (Kafka mode - port 19092)"
	@echo "  p3-failure-test       - Run failure injection test"
	@echo "  p3-metrics            - Check recovery metrics"
	@echo "  p3-metrics-all        - Check all Person 3 metrics"
	@echo "  p3-clean-data         - Clean test data (snapshots, manifest, changelog)"
	@echo "  p3-test-all           - Run complete test suite"
	@echo "  p3-integration-test   - Run integration test with original snapshots"