GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2
BUF := go run github.com/bufbuild/buf/cmd/buf@v1.73.0

.PHONY: build test lint gen

build:
	go build ./...

test:
	go test -race ./...

lint:
	$(BUF) lint
	$(GOLANGCI_LINT) run

gen:
	$(BUF) generate
