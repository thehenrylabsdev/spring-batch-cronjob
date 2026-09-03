# Benchmark Evidence & Chosen Defaults

## How to reproduce

```bash
# Generate and process 1M / 10M representative records (SPEC 61.5):
./mvnw test -Dperf.excluded.groups= -Dperf.records=1000000
./mvnw test -Dperf.excluded.groups= -Dperf.records=10000000

# Full matrix (chunk x workers x thread model) — see docs/benchmarks/matrix.md:
# run the perf test with -Dbatch.processing.chunk-size / -Dbatch.processing.executor.* overrides
```

Results are appended to `target/perf-results.csv`. For release qualification also capture:

- JFR: `-XX:StartFlightRecording=duration=10m,filename=jfr/run.jfr`
- GC: `-Xlog:gc*:file=gc/run.log`
- Micrometer scrape: `/actuator/prometheus` during the run
- executor queue depth gauge: `batch_executor_queue_depth` (must stay near stable plateau)

## Measured results (local development, macOS, 8 workers, chunk 2000)

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
| workers (core/max) | 8 / 8 | ≥ P with bounded queue (16) = backpressure without oversubscription |
| queue-capacity | 16 | bounded in-flight chunks ⇒ memory ≈ chunk × in-flight (SPEC 57) |
| partitioning.mode | SINGLE_READER | Level-1 architecture; evidence below |

## Tuning order evidence

Per SPEC 62: sequential baseline → chunk size → parser → processor → reference data →
workers → DB checkpoint overhead → partitioning. The measured 10M result uses only
Level 1 (single sequential reader + concurrent chunks) — see ADR-0003/0009.

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
(upstream shards via `MultiResourcePartitioner`); the property seam
`batch.processing.partitioning.mode` already exists to switch without redesign.
