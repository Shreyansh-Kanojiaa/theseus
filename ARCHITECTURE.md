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

Every record carries `node_id`, a persisted monotonic `seq`, a hybrid logical clock
timestamp and a priority (incident > action > event > metric). Dedupe is on
`(node_id, seq)`; fleet ordering is by HLC, so a late node cannot rewrite history.

## Recovery ladder (TCRA)

Rule engine → dependency graph (≤8 candidate causes) → Laya (calibrated choice, acts at
p ≥ 0.85, reversible actions only) → on-demand LLM → human. One deterministic executor
performs and verifies every action; a failed verification rolls back and escalates.

## Sync

Connected → Buffering → Handshake (watermark) → Replay (resumable, priority-ordered) →
Reconcile (dedupe, HLC order). Month 1 uses naive full-state sync behind a `Syncer`
interface as the measured baseline.
