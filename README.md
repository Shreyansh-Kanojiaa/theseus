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

## Layout

| Dir             | Module                                             |
|-----------------|----------------------------------------------------|
| `agent/`        | Edge agent: collector, local store, probes, alerts |
| `recovery/`     | Recovery engine (rules, graph, Laya, LLM, executor)|
| `sync/`         | Synchronization engine                             |
| `incident/`     | Incident timeline reconstruction                   |
| `chaos/`        | Fault injection and ground-truth labels            |
| `dashboard/`    | Grafana dashboards, provisioned as code            |
| `controlplane/` | Optional control plane                             |

## License

[Apache 2.0](LICENSE).
