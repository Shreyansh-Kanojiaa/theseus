#!/usr/bin/env bash
# Month 1 acceptance test (CP11). From a fresh testbed:
#   1. bring up nodes a, b, c and the control plane
#   2. sever C for SEVER_MINUTES (default 30)
#   3. kill Prometheus on C at the start, fill C's disk halfway through
#   4. check C kept collecting and fired local alerts, with no help from outside
#   5. revert, reconnect, and confirm the control plane has every record C wrote
# It prints the numbers for BENCHMARKS.md. Needs docker and sqlite3 on the host.
set -euo pipefail
cd "$(dirname "$0")/.."

SEVER_MINUTES=${SEVER_MINUTES:-30}
C=bin/theseus-chaos
AGENT=theseus-c-agent-1
CP=theseus-cp-controlplane-1
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

fails=0
step() { printf '\n== %s  %s\n' "$(date -u +%T)" "$*"; }
pass() { printf '   PASS  %s\n' "$*"; }
fail() { printf '   FAIL  %s\n' "$*"; fails=$((fails + 1)); }
check() { local what=$1; shift; if "$@"; then pass "$what"; else fail "$what"; fi; }
metric() { # metric <name{labels}>: agent C's own /metrics, read inside the node
	docker exec "$AGENT" wget -qO- localhost:9101/metrics | awk -v k="$1" '$1 == k {print $2}'
}
written() { docker exec "$AGENT" wget -qO- localhost:9101/metrics | awk -F'[{} ]' '$1 == "theseus_agent_records_written_total" {n += $NF} END {print n + 0}'; }
gt() { [ "${1:-0}" -gt "${2:-0}" ]; }

command -v sqlite3 >/dev/null || { echo "sqlite3 is needed on the host" >&2; exit 2; }

step "fresh testbed"
make -s down >/dev/null 2>&1 || true
make -s up >/dev/null
go build -o "$C" ./cmd/theseus-chaos
rm -rf .chaos
sleep 60 # a few collection and sync rounds while connected
check "C syncs while connected" sh -c "docker logs $CP 2>&1 | grep -q 'upload from c'"

step "sever C for $SEVER_MINUTES min, kill its Prometheus"
make -s sever NODE=c >/dev/null
t0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
w0=$(written)
"$C" inject kill --node c --target prometheus

sleep $((SEVER_MINUTES * 30))
step "halfway: fill C's disk"
w1=$(written)
check "C kept writing records while severed ($w0 -> $w1)" gt "$w1" "$w0"
"$C" inject disk-fill --node c

sleep $((SEVER_MINUTES * 30))
step "end of the outage, still severed"
w2=$(written)
check "C kept writing records on a full disk ($w1 -> $w2)" gt "$w2" "$w1"
check "C is in disk-full degraded mode" [ "$(metric theseus_agent_disk_degraded)" = 1 ]
check "C counted Prometheus probe failures" gt "$(metric 'theseus_agent_probe_failures_total{target="theseus-c-prometheus-1"}')" 2
check "C fired disk_high locally (agent stdout)" \
	sh -c "docker logs --since $t0 $AGENT 2>/dev/null | tr -d ' ' | grep -q '\"kind\":\"alert\",\"source\":\"disk_high\"'"
check "nothing from C reached the control plane" \
	sh -c "! docker logs --since $t0 $CP 2>&1 | grep -q 'upload from c'"
rss=$(docker exec "$AGENT" awk '/^VmRSS/ {print $2}' /proc/1/status)
hwm=$(docker exec "$AGENT" awk '/^VmHWM/ {print $2}' /proc/1/status)

step "revert faults, reconnect C"
"$C" revert disk-fill --node c
"$C" revert kill --node c --target prometheus
for _ in $(seq 18); do # the store checks for room every 30 s
	[ "$(metric theseus_agent_disk_degraded)" = 0 ] && break
	sleep 5
done
check "C left degraded mode on its own" [ "$(metric theseus_agent_disk_degraded)" = 0 ]
make -s heal NODE=c >/dev/null
line=""
for _ in $(seq 36); do # up to 3 min
	line=$(docker logs --since "$t0" "$AGENT" 2>&1 | grep "reconnected" | tail -1) && [ -n "$line" ] && break
	sleep 5
done
check "C reconnected and synced its backlog" [ -n "$line" ]
echo "   $line"

step "does the control plane have all of C's data?"
docker cp -q "$AGENT:/disk/agent/." "$WORK/agent/"
sleep 35 # one more sync, so everything in that snapshot has been sent
docker cp -q "$CP:/data/." "$WORK/cp/"
q() { sqlite3 "$WORK/agent/theseus.db" "ATTACH '$WORK/cp/controlplane.db' AS cp; $1"; }
held=$(q "SELECT count(*) FROM (SELECT seq FROM samples UNION ALL SELECT seq FROM events)")
missing=$(q "SELECT count(*) FROM (SELECT seq FROM samples UNION ALL SELECT seq FROM events) a
	WHERE NOT EXISTS (SELECT 1 FROM cp.records r WHERE r.node_id = 'c' AND r.seq = a.seq)")
ev() { q "SELECT count(*) FROM events WHERE kind = '$1' AND source = '$2'"; }
check "all $held records C holds are at the control plane ($missing missing)" [ "$missing" = 0 ]
check "probe_fail for Prometheus is among them" gt "$(ev probe_fail theseus-c-prometheus-1)" 0
check "the disk_high alert is among them" gt "$(ev alert disk_high)" 0
check "the agent_disk_full incident is among them" gt "$(ev incident agent)" 0
check "agent_disk_recovered is among them" gt "$(ev agent_disk_recovered agent)" 0
check "chaos ground truth: 2 labelled faults" [ "$(wc -l < .chaos/ground-truth.jsonl)" = 2 ]

step "numbers for BENCHMARKS.md"
echo "   outage:         ${SEVER_MINUTES} min (C severed; Prometheus killed; disk full for the second half)"
echo "   reconnect sync: ${line#*reconnected }"
echo "   agent C RSS:    $((rss / 1024)) MiB at the end of the outage, peak $((hwm / 1024)) MiB"

if [ "$fails" -gt 0 ]; then
	printf '\nFAILED: %d check(s)\n' "$fails"
	exit 1
fi
printf '\nPASSED: month 1 acceptance\n'
