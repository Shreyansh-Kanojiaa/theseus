# Theseus

Autonomous, offline-first SRE platform for edge infrastructure. Every node runs the full
observe → decide → act loop locally; the control plane is optional and a node loses
visibility, not autonomy, when its uplink drops. Guiding rule: **use the least intelligence
that can decide.** Source: `Theseus_Systems_Design_Report_v2` (revision 2).

## Design summary

- **Edge agent** (`agent/`): collector (metrics, logs, probes), local store (SQLite, 7-day
  retention), service discovery via Docker labels, local alerts, sync spool.
- **Recovery engine** (`recovery/`), the TCRA ladder, each rung runs only if the one below
  gave up:
  1. Rule engine: deterministic, <1 ms, expected to resolve ~93%.
  2. Dependency graph: resolves nothing itself, narrows candidates to ≤8 causes.
  3. Laya (ModernBERT-large 421M, `laya-typed-decisions`): typed choice + "none of these",
     calibrated, acts only at p ≥ 0.85 and only via reversible allow-listed actions
     (evict cache, restart, config rollback, drain). Must be fine-tuned on chaos data and
     temperature-calibrated before it may act. ~6%.
  4. On-demand LLM (1–3B GGUF via llama.cpp), loaded then released. ~1%. Then page a human.
  One deterministic executor performs and verifies every action, whichever rung chose it.
  Failed verify → roll back and escalate. Retries bounded (3 attempts, 2/4/8 s backoff).
- **Sync engine** (`sync/`): states Connected → Buffering → Handshake (watermark = highest
  contiguous seq the centre holds) → Replay (resumable chunks, priority drain: incidents >
  actions > events > metrics) → Reconcile (dedupe on (node_id, seq), order by HLC). Writes
  never block; near spool cap, oldest low-priority samples are downsampled; incidents are
  never dropped. Month 1 ships naive full-state sync behind a `Syncer` interface as baseline.
- **Incident intelligence** (`incident/`): timeline + narrative after reconnect, recording
  which rung decided and with what confidence.
- **Chaos engine** (`chaos/`): inject/revert faults; every injection writes a ground-truth
  JSONL record, which is Laya's training label.
- **Dashboard** (`dashboard/`): Grafana provisioned as code; topology, sync queue, rung breakdown.
- **Control plane** (`controlplane/`): gRPC sync endpoint, fleet view, timeline archive.

Telemetry target: 3.1 MB/h/node vs 42 MB/h constant scraping, anomaly latency unchanged.
Plan: month 1 foundation (offline monitoring works), month 2 recovery + Laya (cluster heals
itself), month 3 delta sync, telemetry priority, LLM tier, ablations, paper and demo.

## Conventions

- Go, single module `github.com/Shreyansh-Kanojiaa/theseus`, one package dir per module.
- `make build test lint` must stay green; CI runs exactly that.
- Package `sync` shadows stdlib: import stdlib as `gosync "sync"` inside it if needed.
- Records live in `schema/v1/records.proto` (Go package `schemav1`). Edit the proto, then
  `make gen`; generated code is committed and CI fails if it is stale. Only add fields,
  never renumber or reuse tags. `buf lint` runs as part of `make lint`.
- Every record has a `Header`: node_id, seq, hlc, priority. seq is one per-node counter
  shared by all record types and is **never reused**; gaps are allowed (crash, downsampling),
  so sync must not assume contiguity without the node declaring dropped ranges.
- HLC is a uint64: unix ms << 16 | logical. Compare as integers. `schemav1.Clock` issues them.
- `agent.Stamper` assigns node_id/seq/hlc and fsyncs the last seq+hlc before returning, so
  seq and hlc survive restarts and wall-clock resets. Stamp in batches (two fsyncs per call).
- `agent.Store` (SQLite, pure-Go `modernc.org/sqlite`, WAL): `Append(records...)` is the only
  write path. It stamps, then writes each record to `samples` or `events` and to `spool` in
  one transaction. `schemav1.Record` is the oneof envelope stored in the spool and sent on
  the wire. `Prune`/`RunRetention` age out samples and events (7 days); the spool is only
  drained by sync acks. Retention relies on hlc rising with seq (see `Prune`).
- Collectors are `agent.CollectFunc`s run by `agent.Collect` (one goroutine and one Append
  per round each). Docker is spoken to over its socket with net/http, no SDK.
  Binaries live in `cmd/`. Health probes come from the `theseus.probe` container label
  (`agent.Probes`); the Nth consecutive miss emits a `probe_fail` event.
  `agent.LogTail` polls the logs of containers labelled `theseus.logs` or `theseus.probe`,
  keeps the last N lines each in memory (`Lines`, for month 2's evidence bundle) and
  emits one `log_match` event per container and keyword per round.
- Local alerts: `agent.Alerts.Watch` wraps a CollectFunc and evaluates its samples against
  YAML threshold rules (`agent/alerts.yaml` embedded as the default) before they reach the
  store, so alerts survive degraded mode. Transitions emit `alert` / `alert_resolved`
  events, stored and sent to stdout or a webhook. YAML is `go.yaml.in/yaml/v3`.
- Disk-full survival (`agent/diskfull.go`): the store keeps `<data>/ballast` (fallocated,
  `-ballast`, default 64 MiB). The first ENOSPC/SQLITE_FULL deletes it and enters degraded
  mode: an `agent_disk_full` incident (OPEN) is written, non-essential samples are dropped
  before stamping (and counted), everything else is still written, or held in a 10k
  in-memory queue (lowest priority evicted first) if even that fails. Essential samples
  (`host_disk_*`, `host_memory_*`, `up`, `container_running`, and metrics named by alert
  rules via `Store.KeepSamples`) are kept: they are the incident's evidence. `Append` does
  not return disk-full errors. Every 30 s it checks for 2x ballast + 16 MiB free, recreates
  the ballast, and writes the RESOLVED incident (same incident_id, opened_hlc = the OPEN
  record's stored hlc) with `agent_disk_recovered` in one batch. A restart mid-episode
  reuses the open incident (still full) or resolves it (room). Test on a real tmpfs with
  `make test-diskfull`.
- Testbed (`testbed/node.yaml`, one compose project per node, `make up/down/sever/heal`):
  each node's services sit on an internal network and share a 1 GiB tmpfs `/disk`; only
  the agent joins `theseus-uplink`, so `make sever NODE=c` is one network disconnect.
  Agents share the host's Docker daemon and see only their node via `-docker-label`.
- Agent `/metrics` (`-listen`, default :9101) is hand-written Prometheus text from
  `Store.WriteMetrics`, no client_golang. Grafana (`testbed/controlplane.yaml`, port 3300)
  is provisioned only from `dashboard/`; edit the dashboard JSON, never the UI.
- Sync: `sync.Syncer` is the seam; `sync.Naive` (month 1 baseline) uploads the whole
  spool over gRPC (`SyncService.Upload` in `schema/v1/sync.proto`, generated with
  `protoc-gen-go-grpc` as a go tool) every `-sync-interval`, and `Store.AckSpool` trims it
  only after the control plane answers. `controlplane.Server` dedupes on (node_id, seq).
  Every sync logs records, payload/wire bytes and duration; keep that, it is the baseline.
  Keepalive + capped backoff exist because a severed uplink blackholes, it doesn't reset.
- Chaos (`chaos/`, `cmd/theseus-chaos inject|revert|list`): faults kill, netem-loss,
  uplink-drop, disk-fill on a testbed node via the docker CLI; network faults run tc/iptables
  in a helper sharing the agent's netns. Each fault has an `active` check run after every
  inject and revert. Active faults live in `.chaos/active/`; each revert appends one
  `{fault,node,target,params,start,end}` line to `.chaos/ground-truth.jsonl` (Laya's
  labels, gitignored). disk-fill refuses any volume that isn't the testbed tmpfs.
- Month 1 acceptance: `make acceptance` (`testbed/acceptance.sh`, `SEVER_MINUTES`, default 30)
  runs the CP11 scenario from a fresh testbed and exits non-zero if any check fails. It needs
  sqlite3 on the host and holds the machine awake with systemd-inhibit.
- Record numbers in BENCHMARKS.md with the command that reproduces them. `make bench`.

## Month 1 checkpoints

Week 1: skeleton, schema, store

CP0: Repo skeleton + tooling. Monorepo with one dir per module (agent/, recovery/, sync/, incident/, chaos/, dashboard/, controlplane/), a Makefile or justfile, golangci-lint, CI running lint + tests, and stub docs (README, ARCHITECTURE, ROADMAP). Also add a CLAUDE.md that summarizes the design report and lists these checkpoints so every Claude Code session starts with context. Done when: make build test lint is green in CI.

CP1: Event schema (protobuf). Define Sample, Event, Incident, and ProbeResult. Every record carries node_id, a persisted monotonic seq, a hybrid logical clock timestamp, and a priority (incident > action > event > metric). This is the most important checkpoint in month 1. If you get this right now, month 2's delta sync is an addition. If you skip it, month 2 becomes a rewrite. Done when: codegen works, round-trip tests pass, and seq survives an agent restart.

CP2: Local store. SQLite in WAL mode with tables for samples, events and the sync spool, plus a 7-day retention job. Done when: a benchmark does 100k writes, the retention job is tested, and kill -9 mid-write doesn't corrupt the DB.

Week 2: the agent actually sees things

CP3: Collector. Host metrics (CPU, mem, disk, net) from gopsutil/procfs, container state from the Docker API, and scraping of node_exporter into the local store. Intervals are configurable. Done when: the agent runs for 1 hour, the store has data, and the agent's own RSS stays reasonable (measure it and write the number down).

CP4: Service discovery + health probes. Discover containers through Docker labels (e.g. theseus.probe=http://:9090/-/healthy), support HTTP/TCP/exec probes, and track consecutive failures ("3 misses over 30s" from Figure 2). Done when: you stop a container and see a probe_fail x3 event appear.

CP4.5: Disk-full survival. The store shares the disk the chaos engine fills, and on ENOSPC every Append failed, incidents included. Add a fallocated ballast file freed on the first disk-full error, a degraded mode that drops samples but keeps events, probes and incidents, a bounded in-memory queue for when even those fail, and recovery that recreates the ballast. Done when: make build test lint is green in CI with the 16 MiB tmpfs test running, not skipped.

CP4.6: Disk-full evidence and closure. Degraded mode dropped every sample, including the disk readings its own incident needs as evidence, and the agent_disk_full incident never closed. Keep essential samples (disk, memory, up, container_running, alert-rule metrics) while degraded, export theseus_agent_samples_dropped_total, resolve the incident on recovery under the same incident_id with the OPEN record's stored hlc, and never open a second one across a restart. Done when: make build test lint is green in CI with TestDiskFullTinyFS running, and a full make acceptance passes with the new disk-reading and incident checks.

CP5: Log tail. Keep a ring buffer of the last N log lines per labelled container, and turn keyword matches ("No space left on device", "OOM") into events. Keep it small. Its real job is feeding Laya's evidence bundle in month 2.

Week 3: deployment and local alerting

CP6: Local alerts. YAML threshold rules produce alert events written locally, with stdout or a local webhook as the output. There is no recovery yet, only detection. Done when: disk > 85% fires an alert with the network unplugged.

CP7: 3-node testbed. An agent Dockerfile and a compose setup where each "node" is agent + postgres + prometheus + node_exporter. Give each node its own network so you can sever one uplink cleanly, and set mem/CPU limits to mimic edge hardware (1 to 2 GB, 1 to 2 CPUs). Done when: make up brings up nodes A, B and C, and make sever NODE=c cuts C off.

CP8: Prometheus + Grafana wiring. The agent exposes its own /metrics (samples written, spool depth, probe failures). Grafana runs on the control plane side, with dashboards and datasources provisioned as code and nothing clicked together in the UI. Done when: a fresh make up shows working dashboards with zero manual setup.

Week 4: sync baseline, chaos, acceptance

CP9: Control plane stub + naive full-state sync. A gRPC server where a node uploads everything on reconnect. Put it behind a Syncer interface so delta sync slots in later. Log the bytes transferred and the duration on every sync, because the naive version is your baseline for the bandwidth and sync experiments. Treat it as your first data point, not throwaway code.

CP10: Chaos harness groundwork. A CLI like theseus-chaos inject <fault> --node c --target prometheus with a matching revert. Start with 4 faults: kill container, tc netem loss, iptables uplink drop, and fallocate disk fill. Every injection writes a ground-truth JSONL record (fault type, target, start, end). That record is the future Laya training label, so capturing it now costs you nothing. Done when: each fault injects and reverts cleanly, twice in a row.

CP11: Month 1 acceptance test. Write one script that runs the whole scenario:

Bring up 3 nodes.
Sever C for 30 minutes.
Kill Prometheus on C and fill C's disk.
Check that C kept collecting and fired local alerts.
Reconnect and confirm the control plane has all of C's data.

Write the numbers (sync bytes, sync time, agent RSS) into BENCHMARKS.md. When this script passes, month 1 is done.
