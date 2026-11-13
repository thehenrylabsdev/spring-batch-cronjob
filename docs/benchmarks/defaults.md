# Benchmark Evidence & Chosen Defaults

## How to reproduce

```bash
# Generate and process 1M / 10M representative records (SPEC 61.5):
./mvnw test -Dperf.excluded.groups= -Dperf.records=1000000
./mvnw test -Dperf.excluded.groups= -Dperf.records=10000000

# Chunk-size overrides: -Dbatch.processing.chunk-size=N (honoured by the distributed workers).
# The executor overrides (-Dbatch.processing.executor.*) have no effect on the current job; the
# full matrix (chunk x workers x thread model) is not in the repository yet.
```

Results are appended to `target/perf-results.csv`. For release qualification also capture:

- JFR: `-XX:StartFlightRecording=duration=10m,filename=jfr/run.jfr`
- GC: `-Xlog:gc*:file=gc/run.log`
- Micrometer scrape: `/actuator/prometheus` during the run
- executor queue depth gauge: `batch_executor_queue_depth` (must stay near stable plateau)

## Measured results (local development, macOS, chunk 2000)

The worker count for these figures is not recorded, and `target/perf-results.csv` is generated, not committed, so the
numbers cannot be reproduced from the repository as it stands. `PerformanceQualificationTest` runs the distributed job
with the default of one worker slot per pod.

| records | elapsed | records/sec | SLA (60 min) headroom |
|---|---|---|---|
| 1,000,000 | 2 s | ~500,000 | ~2,000x |
| 10,000,000 | 18 s | ~556,000 | ~200x |

The 10M representative dataset includes quoted fields, commas inside quotes, multiline
quoted fields, unicode names, malformed amounts (INVALID), suppressed rows (SKIPPED) and
out-of-range dates (REJECTED_RULE_B) — all classified to completion and reconciled.

## Chosen defaults (SPEC 62 tuning order, 21, 28)

| Setting | Default | Rationale |
|---|---|---|
| chunk-size | 2000 | mid-range checkpoint granularity; 500/1000/5000/10000 remain configurable for the matrix |
| worker slots per pod | 1 (`batch.distributed.worker.concurrency`) | Current default. Each slot runs one partition at a time; raise only after benchmarking. |
| executor core/max | 8 / 8 (`batch.processing.executor`) | Bound and create `batchProcessingExecutor`, but no code submits work to it. Not in effect. |
| queue-capacity | 16 | Same executor as above; no effect on the current job. |
| partitioning.mode | SINGLE_READER | Bound but not read. Partitioning is always the coordinator's byte-range plan (ADR-0011). |

## Tuning order evidence

Per SPEC 62: sequential baseline → chunk size → parser → processor → reference data →
workers → DB checkpoint overhead → partitioning. The 10M result comes from the distributed
job with one worker slot, so it measures the byte-range partitioned path run sequentially
on one pod, not the in-JVM concurrent-chunk executor described in ADR-0004.

- Parser: univocity streaming parser handles the full quoted/multiline contract with no
  byte-offset hacks (ADR-0005).
- Processor: stateless classification against a preloaded in-memory reference snapshot —
  zero per-record remote calls (SPEC 25).
- DB checkpoint overhead: one metadata transaction per chunk (2000 records).

## When to revisit (SPEC 8)

Only if a production measurement shows:

1. reader utilization proven as the bottleneck **and**
2. processing workers consistently starved

…and only then consider Level 2 (`SAFE_BOUNDARIES` / indexed record boundaries) or Level 3
(upstream shards via `MultiResourcePartitioner`). The property `batch.processing.partitioning.mode` is bound
but not read, so switching it has no effect yet.
