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

Label a container to have it health-probed (every 10 s; 3 misses emit `probe_fail`):

```sh
docker run -l 'theseus.probe=http://:9090/-/healthy' prom/prometheus
docker run -l 'theseus.probe=tcp://:5432' ...                   # or exec:pg_isready -q
```

## Layout

| Dir             | Module                                             |
|-----------------|----------------------------------------------------|
| `agent/`        | Edge agent: collector, local store, probes, alerts |
| `cmd/`          | Binaries (`theseus-agent`)                         |
| `recovery/`     | Recovery engine (rules, graph, Laya, LLM, executor)|
| `sync/`         | Synchronization engine                             |
| `incident/`     | Incident timeline reconstruction                   |
| `chaos/`        | Fault injection and ground-truth labels            |
| `dashboard/`    | Grafana dashboards, provisioned as code            |
| `controlplane/` | Optional control plane                             |

## License

[Apache 2.0](LICENSE).
