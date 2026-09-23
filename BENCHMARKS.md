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
