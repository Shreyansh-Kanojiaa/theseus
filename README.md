# Theseus

An autonomous, offline-first SRE platform for edge infrastructure. Each node detects,
diagnoses and recovers from failures locally, with or without a network link, using the
least intelligence that can decide: rules first, then a small calibrated classifier (Laya),
then an on-demand LLM.

Status: month 1, foundation. See [ROADMAP.md](ROADMAP.md) and [ARCHITECTURE.md](ARCHITECTURE.md).

## Develop

Requires Go (version in `go.mod`).

```sh
make build test lint
make gen    # after editing schema/v1/records.proto
```

Run the agent against a local node_exporter:

```sh
go run ./cmd/theseus-agent -data data -scrape http://localhost:9100/metrics
go run ./cmd/theseus-agent -h    # intervals, mounts, procfs root, Docker socket
```

Label a container to have it health-probed (every 10 s; 3 misses emit `probe_fail`) and
its logs tailed (keyword matches emit `log_match`):

```sh
docker run -l 'theseus.probe=http://:9090/-/healthy' prom/prometheus
docker run -l 'theseus.probe=tcp://:5432' ...                   # or exec:pg_isready -q
docker run -l theseus.logs=1 ...                                 # logs only, no probe
```

Local alerts are threshold rules in YAML (`-alerts`, default `agent/alerts.yaml`, e.g.
disk > 85%). Alert events are stored and printed to stdout as JSON lines, or sent to
`-alert-output http://localhost:.../hook`.

3-node testbed (Docker Compose): `make up`, then `make sever NODE=c` / `make heal NODE=c`
to cut and restore node C's uplink, `make down` to remove everything. The fleet dashboard
is at http://localhost:3300 (Grafana, no login; `GRAFANA_PORT` to move it). Every agent
uploads its spool to the control plane every 30 s; `docker logs theseus-c-agent-1` shows
each sync's records, bytes and duration.

Break node C on purpose (each command checks the fault took effect, and its revert
that it is gone; ground truth goes to `.chaos/ground-truth.jsonl`):

```sh
go build -o bin/theseus-chaos ./cmd/theseus-chaos
bin/theseus-chaos inject kill --node c --target prometheus   # also: netem-loss, uplink-drop, disk-fill
bin/theseus-chaos revert kill --node c --target prometheus
make chaos-check                                             # all four, twice each
```

Month 1 acceptance, about 35 minutes: `make acceptance` (`SEVER_MINUTES=3` for a quick run).

## Layout

| Dir             | Module                                             |
|-----------------|----------------------------------------------------|
| `agent/`        | Edge agent: collector, local store, probes, alerts |
| `cmd/`          | Binaries (`theseus-agent`)                         |
| `recovery/`     | Recovery engine (rules, graph, Laya, LLM, executor)|
| `sync/`         | Sync engine: `Syncer`, naive full upload (month 1) |
| `incident/`     | Incident timeline reconstruction                   |
| `chaos/`        | Fault injection and ground-truth labels            |
| `dashboard/`    | Grafana dashboards, provisioned as code            |
| `controlplane/` | Optional control plane: gRPC sync endpoint         |

## License

[Apache 2.0](LICENSE).
