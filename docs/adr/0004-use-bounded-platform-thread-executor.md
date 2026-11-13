# ADR-0004: Bounded platform-thread executor

## Status
Accepted. Implementation note: `batchProcessingExecutor` is defined and exposes gauges, but no code submits chunks to
it. Distributed workers run one partition per worker slot (`batch.distributed.worker.concurrency`, default 1), so the
core/max=8 values below are not in effect. See README "Implementation status".

## Decision
A dedicated `batchProcessingExecutor` (platform threads, core=max=8, bounded queue 16,
CallerRunsPolicy) executes chunks. Virtual threads remain OFF for the CPU-oriented batch
path (SPEC 31); Java 25 virtual threads may be benchmarked for blocking helper workflows.

## Context
CSV parsing + classification are CPU-bound; throughput is limited by processor cores, and
bounded concurrency yields predictable memory (SPEC 57) and backpressure (SPEC 30).

## Consequences
- No unbounded task queue anywhere (AC-006); no thread-per-record (AC-007).
- Backpressure: queue saturation runs the chunk on the reader thread (CallerRuns).
- Metrics: `batch_executor_active_threads`, `batch_executor_queue_depth`.
- The dedicated executor is never reused by Quartz, web or `@Async` (SPEC 29).
