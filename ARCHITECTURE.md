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

A failing source logs once and keeps retrying; the other loops are unaffected.

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

- an `agent_disk_full` incident is appended;
- samples are dropped before they are stamped (no seqs burnt) and counted;
- events, probe results and incidents are still written;
- if even those fail (the fault refilled the freed space), they wait in memory, up to
  10k records, lowest priority evicted first, and are stamped when they finally commit.

Every 30 s a degraded store checks for 2x ballast + 16 MiB free. When it finds it, it
recreates the ballast, leaves degraded mode and appends `agent_disk_recovered` with the
number of dropped samples. A failed write's seqs are skipped, never reused.

## Sync

Connected → Buffering → Handshake (watermark) → Replay (resumable, priority-ordered) →
Reconcile (dedupe, HLC order). Month 1 uses naive full-state sync behind a `Syncer`
interface as the measured baseline.
