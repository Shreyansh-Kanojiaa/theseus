# Benchmarks

Numbers are measured, not assumed. Each row names the command that reproduces it.

## Local store (CP2)

`make bench` → `BenchmarkAppend100k`: 100k samples per op, batches of 1000 (about one
node_exporter scrape), SQLite WAL + `synchronous=NORMAL`, pure-Go driver.

| Date       | Machine                     | Time per 100k | Records/s |
|------------|-----------------------------|---------------|-----------|
| 2026-09-23 | i5-13450HX, NVMe, Fedora 44 | 0.82 s        | ~123k     |

Each batch costs two fsyncs for the stamp file plus the SQLite commit, so small batches
are fsync-bound. Batch writes.

## Agent soak (CP3)

`bin/theseus-agent -data $D -scrape http://localhost:9100/metrics` for 1 hour: host
metrics and container state every 15 s (16 containers), node_exporter (`--net=host
--pid=host`, ~1,830 series) every 30 s. RSS read from `/proc/$PID/status` every 5 min.

| Date       | Machine                     | RSS at 5 min | RSS at 60 min | Peak    | Records/h | DB after 1 h |
|------------|-----------------------------|--------------|---------------|---------|-----------|--------------|
| 2026-09-24 | i5-13450HX, NVMe, Fedora 44 | 25.4 MB      | 25.4 MB       | 26.6 MB | 233k      | 59 MB        |

RSS is flat after warm-up, so nothing is leaking. The node_exporter scrape is ~95% of the
records. Stored unfiltered, 7 days would be ~10 GB, and the spool grows the same way
until sync drains it. Month 3 downsampling or a scrape allowlist has to fix that before
edge-sized disks.

## Naive sync (CP9)

The month 1 baseline delta sync has to beat. Testbed nodes (`make up`: node_exporter with
5 collectors, host metrics and container state every 15 s, probes every 10 s), each agent
uploading its whole spool every 30 s. Numbers from the agent's own sync log line
(`docker logs theseus-c-agent-1`); wire = bytes read + written on the TCP connection.

Reconnect after a 5-minute sever:
`make up; make sever NODE=c; sleep 300; make heal NODE=c; docker logs theseus-c-agent-1 | grep reconnected`
(keep the host awake, e.g. under `systemd-inhibit`: a suspend in the middle skews it).

| Date       | Machine                     | Outage | Records | Payload | On the wire | Upload time |
|------------|-----------------------------|--------|---------|---------|-------------|-------------|
| 2026-09-28 | i5-13450HX, NVMe, Fedora 44 | 5m0s   | 4,195   | 457 KB  | 466 KB      | 349 ms      |
| 2026-09-28 | i5-13450HX, NVMe, Fedora 44 | 5m0s   | 4,195   | 457 KB  | 466 KB      | 350 ms      |

Steady state (connected, same run): ~380 records per node per sync, ~41 KB payload,
~42 KB on the wire, 10-30 ms. That is ~5 MB/h/node on the wire against the design target
of 3.1 MB/h/node, before any priority or downsampling. gRPC and HTTP/2 framing add ~2%
over the protobuf payload. After the sever the control plane held every record node C
had written, both times: no holes in its seq range.

## Month 1 acceptance (CP11)

`make acceptance` (`testbed/acceptance.sh`): fresh testbed, node C severed for 30 min,
its Prometheus killed at the start and its disk filled for the second half, then
reverted and reconnected. Every check passed; the control plane held all 13,207 records
C had written (0 missing), including the probe_fail, disk_high alert, agent_disk_full
incident and agent_disk_recovered events it recorded while cut off.

| Date       | Machine                     | Outage | Backlog records | Payload | On the wire | Sync time | Agent RSS (peak) |
|------------|-----------------------------|--------|-----------------|---------|-------------|-----------|------------------|
| 2026-09-29 | i5-13450HX, NVMe, Fedora 44 | 30 min | 12,107          | 1.30 MB | 1.33 MB     | 189 ms    | 29 MiB (29 MiB)  |

The backlog is small for 30 minutes because the full disk made C drop samples for 15 of
them, as designed: 11,101 records in the first 15 min, 272 in the second.
