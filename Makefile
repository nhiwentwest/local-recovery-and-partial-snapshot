.PHONY: build build-all test test-race coverage test-integration

build:
	mkdir -p bin
	go build -o bin/opb ./cmd/opb

build-all:
	mkdir -p bin
	go build -o bin/opb ./cmd/opb
	go build -o bin/opa ./cmd/opa
	go build -o bin/genorders ./cmd/genorders
	go build -o bin/pump ./cmd/pump
	go build -o bin/recover ./cmd/recover
	go build -o bin/count_changelog ./cmd/count_changelog
	go build -o bin/bench_latency ./cmd/bench_latency

test:
	go test ./...

test-race:
	go test -race ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report written to coverage.html"

test-integration:
	go test -tags=integration ./...
	@echo "Coverage report written to coverage.html"
