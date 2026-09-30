# Architecture

A lightweight, optional control plane over autonomous edge nodes. No node depends on the
control plane to keep running.

```
            ┌──────────────── control plane (optional) ────────────────┐
            │ fleet view · timeline archive · cross-node correlation   │
            └──────▲──────────────────▲──────────────────▲─────────────┘
         delta sync│                  │                  │ (link down)
     ┌─────────────┴──┐     ┌─────────┴──────┐     ┌─────┴──────────┐
     │ node A         │     │ node B         │     │ node C         │
     │ collector      │     │ ...            │     │ ...            │
     │ local store    │     │                │     │ spool buffers, │
     │ decision       │     │                │     │ loop continues │
     │ actuator       │     │                │     │                │
     │ sync spool     │     │                │     │                │
     └────────────────┘     └────────────────┘     └────────────────┘
```

## Records

Schema: `schema/v1/records.proto` defines `Sample`, `Event`, `ProbeResult` and `Incident`.
Every record carries a `Header`:

| Field      | Meaning                                                                       |
|------------|-------------------------------------------------------------------------------|
| `node_id`  | Originating node                                                              |
| `seq`      | Per-node counter shared by all record types. Never reused; gaps allowed.      |
| `hlc`      | Hybrid logical clock, `unix_ms << 16 \| logical`, compares as an integer      |
| `priority` | incident > action > event > metric; the sync drain order                      |

Dedupe is on `(node_id, seq)`; fleet ordering is by HLC, so a late node cannot rewrite
history. The agent persists the last seq and HLC before using them, so neither repeats
after a restart, a kill -9 or a node booting with a stale wall clock. An incident is a
series of records sharing `incident_id`, one per state change.

## Recovery ladder (TCRA)

Rule engine → dependency graph (≤8 candidate causes) → Laya (calibrated choice, acts at
p ≥ 0.85, reversible actions only) → on-demand LLM → human. One deterministic executor
performs and verifies every action; a failed verification rolls back and escalates.

## Collector

`cmd/theseus-agent` runs one loop per source, each on its own interval, each appending
one batch per round (`agent.Collect`):

- host: CPU, memory, disk and network read straight from procfs and `statfs`
  (`host_*`); `-proc` points at the host's `/proc` when the agent is containerised.
- docker: `container_running{container,image,state}` from the Engine API over the unix
  socket (no SDK).
- scrape: any Prometheus text endpoint, normally node_exporter, stored as-is with an
  `instance` label, plus `up`.

- probe: containers labelled `theseus.probe` (`http://:port/path`, `tcp://:port` or
  `exec:cmd args`; an empty host means the container's IP) are found on every round
  and probed concurrently, each emitting a `ProbeResult` with its consecutive failures.
  The third miss in a row (30 s at the default 10 s) emits a `probe_fail` event, the
  next success `probe_recovered`. A stopped container fails its probe.
- logs: containers labelled `theseus.logs` (any value) or `theseus.probe`, stopped ones
  included, are polled every 5 s for new lines (`timestamps=1`, `since` the newest line
  seen, `tail=100`). The last 100 lines per container stay in memory (`LogTail.Lines`),
  the evidence the recovery engine reads. A line containing a keyword (`-log-keywords`,
  case-sensitive: `No space left on device`, `OOM`, `out of memory`) emits a `log_match`
  event, at most one per container and keyword per round with the match count in
  `attrs.lines`. Lines older than the agent's start fill the buffer but never match.

A failing source logs once and keeps retrying; the other loops are unaffected.

## Local alerts

`agent.Alerts` wraps the sample-producing collectors (host, docker, scrape) and checks
each round's samples against YAML threshold rules (`-alerts`; built-in default
`agent/alerts.yaml`: disk used > 85%, memory > 90% for 1m, `up == 0` for 1m). A rule
names a metric, optional label matchers, an op, a value and an optional `for`. Each
transition becomes an event appended with the round's records, `alert` when it fires
and `alert_resolved` when it clears, one per rule and series, and is also written to
`-alert-output`: stdout as JSON lines, or POSTed to a webhook URL. Rules see samples
before the store does, so alerts keep firing while a full disk has the store dropping
samples. Nothing leaves the node, so alerts work with the uplink cut. Detection
only: nothing acts on an alert yet.

## Testbed

`make up` builds the agent image (`Dockerfile`), starts nodes a, b and c from
`testbed/node.yaml`, one compose project each (`theseus-a`...), and the control-plane
side (below). A node is agent +
postgres + prometheus + node_exporter on the node's own `internal` network, so it has no
route out except through its agent, the only container also on the shared
`theseus-uplink` network. `make sever NODE=c` disconnects C's agent from the uplink,
cutting C off from the other nodes and the internet while its own services keep
talking; `make heal NODE=c` reconnects it. The agent only sees its own node's
containers (`-docker-label com.docker.compose.project=theseus-c`) on the shared daemon.

Each node's four containers share its disk, a 1 GiB tmpfs volume at `/disk` (agent
store, postgres and prometheus data), so filling it hits all of them as a real full
disk would. Per-container limits add up to ~1.3 GB and 1.35 CPUs per node. tmpfs pages
count against the memory limit of whichever container writes them. node_exporter runs
five collectors (~335 series instead of ~1,800). Host metrics and node_exporter still
read the host kernel's CPU and memory, not the node's limits.

## Agent metrics and dashboards

The agent serves its own `/metrics` on `-listen` (default `:9101`), plain Prometheus
text written by `Store.WriteMetrics`: `theseus_agent_records_written_total{type}` and
`theseus_agent_probe_failures_total{target}` (counted as the store writes them, so they
match what is on disk), `theseus_agent_spool_depth` (rows awaiting sync acks) and
`theseus_agent_disk_degraded` with `theseus_agent_samples_dropped_total`.

The control-plane side of the testbed (`testbed/controlplane.yaml`) is the sync endpoint
(`theseus-cp-controlplane-1:50051`, see Sync), a Prometheus that scrapes every agent over
`theseus-uplink`, and Grafana provisioned entirely from
`dashboard/`: the datasource, a dashboard provider and `dashboard/dashboards/*.json`. The
home dashboard, "Theseus fleet", shows per node the uplink (`up`, CUT OFF when severed),
disk degraded, records written, spool depth and probe failures. Anonymous users can
view it; dashboards are not editable in the UI, so every change goes through the JSON.

## Local store

SQLite in WAL mode (`agent/store.go`), one file per node. `samples` and `events` (which
also holds probe results and incidents) serve local queries and are pruned after 7 days.
`spool` holds every record as an encoded `Record` until the control plane acknowledges
it; age never removes spool rows. A record is written to its table and the spool in one
transaction, so a crash cannot leave one without the other.

## Disk-full behaviour

The store shares its disk with the services it watches, and chaos fills that disk on
purpose. `<data>/ballast` holds 64 MiB of preallocated blocks (`-ballast`). The first
write that fails with ENOSPC or SQLITE_FULL deletes it and puts the store in degraded
mode:

- an `agent_disk_full` incident is appended (OPEN);
- non-essential samples are dropped before they are stamped (no seqs burnt) and counted
  (`theseus_agent_samples_dropped_total`);
- essential samples are kept, because they are the incident's evidence: `host_disk_*`,
  `host_memory_*`, `up`, `container_running`, and every metric an alert rule watches
  (passed in with `Store.KeepSamples`);
- events, probe results and incidents are still written;
- if even those fail (the fault refilled the freed space), they wait in memory, up to
  10k records, lowest priority evicted first, and are stamped when they finally commit.

Every 30 s a degraded store checks for 2x ballast + 16 MiB free. When it finds it, it
recreates the ballast, flushes anything queued in memory and leaves degraded mode. It then
writes, in one batch, the RESOLVED version of the incident (same `incident_id`,
`opened_hlc` = the hlc the OPEN record was stored with, which may be later than when the
disk filled if it waited in memory) and `agent_disk_recovered` with the number of dropped
samples. A failed write's seqs are skipped, never reused.

An agent that restarts mid-episode finds the open incident (the latest agent incident in
`events` is an OPEN `agent_disk_full`): if the disk is still full it carries on under that
id, and if it has room it resolves it at once. One episode, one `incident_id`.

## Sync

Target design: Connected → Buffering → Handshake (watermark) → Replay (resumable,
priority-ordered) → Reconcile (dedupe, HLC order).

Month 1 ships the baseline, `sync.Naive`, behind the `sync.Syncer` interface that delta
sync will also implement. Every `-sync-interval` (30 s) the agent streams its whole spool,
in seq order and 256 KiB chunks, to the control plane's `SyncService.Upload`
(`schema/v1/sync.proto`, gRPC). The control plane (`controlplane.Server`,
`cmd/theseus-controlplane`) stores each chunk in one SQLite transaction with
`INSERT OR IGNORE` on `(node_id, seq)` and answers with received / new / duplicate
counts. Only then does the agent delete the spool up to the last seq it sent. While the
uplink is down the spool simply grows; the first sync after it returns carries the
whole backlog.

What makes it naive, on purpose: no handshake (the node never learns what the centre
already holds), no priority order (an incident waits behind every older sample), no
resume (an upload cut off halfway is resent from the start; the centre drops the
repeats and counts them as duplicates). Both sides log every sync's records, bytes
(protobuf payload, and on the agent also bytes on the TCP connection) and duration;
BENCHMARKS.md has the numbers delta sync has to beat.

A cut uplink drops packets rather than resetting connections, so the client sends
keepalive pings during an upload (10 s, 5 s timeout; the server permits them) and caps
gRPC's reconnect backoff at 10 s: a sync fails within ~30 s of a cut instead of hanging,
and the first sync after a heal happens on the next tick. Transport is plaintext and
unauthenticated for now, fine inside the testbed.

## Chaos

`theseus-chaos` (`chaos/`, `cmd/theseus-chaos`) breaks one testbed node on purpose and
labels exactly when. It runs on the host and drives the `docker` CLI:

| Fault         | Inject                                              | Revert                  |
|---------------|-----------------------------------------------------|-------------------------|
| `kill`        | `docker kill` the node's `--target` service          | `docker start` it       |
| `netem-loss`  | `tc qdisc ... netem loss 30%` (`--loss`) on the agent's uplink | delete the qdisc |
| `uplink-drop` | iptables DROP in and out of the agent's uplink       | delete the rules        |
| `disk-fill`   | fallocate every free byte of the node's `/disk`      | delete the file         |

Network faults run `tc`/`iptables` in a throwaway container sharing the node agent's
network namespace (the image carries iproute2 and iptables), so only that node's uplink
is touched. `uplink-drop` is a partition: packets vanish and connections time out,
where `make sever` removes the interface and connections fail at once. `disk-fill` runs
its own container, so the tmpfs pages are not charged to a node service's memory limit,
and refuses any volume that is not the testbed's tmpfs: `docker run -v` would otherwise
create a missing volume on the host disk and fill that.

Every fault has an `active` check, run after each inject and each revert, so a fault
that did not take effect (or did not go away) is an error, not a silent label. An
injected fault is written to `.chaos/active/` before it is applied and survives a
crashed harness; `revert` removes it and appends the fault's ground truth to
`.chaos/ground-truth.jsonl`, one line per fault:

```json
{"fault":"netem-loss","node":"c","target":"uplink","params":{"loss":"30%"},"start":"2026-09-28T08:03:12.3Z","end":"2026-09-28T08:03:19.6Z"}
```

The window is conservative (start before inject, end after the revert is verified).
These lines are Laya's training labels in month 2. `make chaos-check` injects and reverts
all four faults twice in a row on node C.
