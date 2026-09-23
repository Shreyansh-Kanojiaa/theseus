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

## Local store

SQLite in WAL mode (`agent/store.go`), one file per node. `samples` and `events` (which
also holds probe results and incidents) serve local queries and are pruned after 7 days.
`spool` holds every record as an encoded `Record` until the control plane acknowledges
it; age never removes spool rows. A record is written to its table and the spool in one
transaction, so a crash cannot leave one without the other.

## Sync

Connected → Buffering → Handshake (watermark) → Replay (resumable, priority-ordered) →
Reconcile (dedupe, HLC order). Month 1 uses naive full-state sync behind a `Syncer`
interface as the measured baseline.
