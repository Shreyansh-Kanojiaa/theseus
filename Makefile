GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2
BUF := go run github.com/bufbuild/buf/cmd/buf@v1.73.0

.PHONY: build test test-diskfull lint gen bench image up down sever heal chaos-check acceptance

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

# 3-node testbed (testbed/node.yaml): nodes a, b and c, each on its own internal
# network, their agents joined by theseus-uplink, plus the control-plane side
# (testbed/controlplane.yaml: Prometheus + Grafana on http://localhost:3300).
# sever/heal cut and restore one node's uplink: make sever NODE=c.
NODES := a b c
TESTBED = NODE=$$n docker compose -f testbed/node.yaml

image:
	docker build --network host -t theseus .  # host DNS; some networks block it from the bridge

up: image
	docker network inspect theseus-uplink >/dev/null 2>&1 || docker network create theseus-uplink
	docker compose -f testbed/controlplane.yaml up -d --wait
	for n in $(NODES); do $(TESTBED) up -d --wait || exit 1; done

down:
	docker compose -f testbed/controlplane.yaml down -v
	for n in $(NODES); do $(TESTBED) down -v || exit 1; done
	docker network rm -f theseus-uplink

sever:
	docker network disconnect theseus-uplink theseus-$(or $(NODE),$(error set NODE))-agent-1

heal:
	docker network connect theseus-uplink theseus-$(or $(NODE),$(error set NODE))-agent-1

# Every chaos fault injected and reverted twice in a row on node C of a running
# testbed (make up). theseus-chaos checks each inject took effect and each
# revert undid it; the ground truth goes to .chaos/ground-truth.jsonl.
CHAOS_FAULTS := "kill --target prometheus" netem-loss uplink-drop disk-fill

chaos-check:
	go build -o bin/theseus-chaos ./cmd/theseus-chaos
	for i in 1 2; do for f in $(CHAOS_FAULTS); do \
		bin/theseus-chaos inject $$f --node c && sleep 5 && bin/theseus-chaos revert $$f --node c || exit 1; \
	done; done

# Month 1 acceptance (CP11): fresh testbed, C severed for SEVER_MINUTES (default
# 30) with Prometheus killed and its disk filled, then reconnected and checked.
# Held awake where systemd-inhibit exists: a suspend would stall every node.
acceptance:
	$(if $(shell command -v systemd-inhibit),systemd-inhibit --what=sleep:idle --why="theseus acceptance") testbed/acceptance.sh
