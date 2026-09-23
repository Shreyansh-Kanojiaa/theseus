# Roadmap

| Phase   | Focus                                  | Deliverable                                     |
|---------|----------------------------------------|-------------------------------------------------|
| Month 1 | Foundation                             | Offline monitoring works                        |
| Month 2 | Autonomous recovery, Laya tier         | A cluster that heals itself, mostly without models |
| Month 3 | Delta sync, LLM tier, evaluation       | Paper, benchmarks, demo                         |

## Month 1 checkpoints

Full definitions of done are in [CLAUDE.md](CLAUDE.md).

- [x] CP0 Repo skeleton + tooling
- [x] CP1 Event schema (protobuf)
- [ ] CP2 Local store (SQLite WAL, retention)
- [ ] CP3 Collector
- [ ] CP4 Service discovery + health probes
- [ ] CP5 Log tail
- [ ] CP6 Local alerts
- [ ] CP7 3-node testbed
- [ ] CP8 Prometheus + Grafana wiring
- [ ] CP9 Control plane stub + naive full-state sync
- [ ] CP10 Chaos harness groundwork
- [ ] CP11 Month 1 acceptance test
