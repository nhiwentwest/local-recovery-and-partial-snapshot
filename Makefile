.PHONY: test test-race coverage test-integration

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
