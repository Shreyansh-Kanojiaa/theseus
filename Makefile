GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2
BUF := go run github.com/bufbuild/buf/cmd/buf@v1.73.0

.PHONY: build test test-diskfull lint gen bench

build:
	go build ./...

test:
	go test -race $(TESTFLAGS) ./...

# The disk-full test on a real 16 MiB tmpfs, in a container so it needs no sudo.
# A static test binary runs in plain alpine; label=disable lets SELinux allow the mount.
test-diskfull:
	CGO_ENABLED=0 go test -c -o bin/agent.test ./agent
	docker run --rm --security-opt label=disable --tmpfs /tiny:size=16m -e THESEUS_TINY_FS=/tiny \
		-v $(CURDIR)/bin:/t:ro alpine /t/agent.test -test.run '^TestDiskFullTinyFS$$' -test.v

lint:
	$(BUF) lint
	$(GOLANGCI_LINT) run

gen:
	$(BUF) generate

bench:
	go test -run='^$$' -bench=. -benchtime=3x ./...
