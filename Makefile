GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2

.PHONY: build test lint

build:
	go build ./...

test:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run
