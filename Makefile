.PHONY: test test-unit test-integration build format install release-snapshot govulncheck

BIN_DIR := bin
BINARY := pgmq

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY) ./cmd/pgmq

install:
	go install ./cmd/pgmq

release-snapshot:
	goreleaser release --snapshot --clean

format:
	go fmt ./...

test: test-unit test-integration

test-unit:
	go test ./...

test-integration:
	go test -tags=integration ./tests/integration

govulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
