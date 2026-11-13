# ADR-0007: Chunk checkpoints for restartability

## Status
Accepted

## Decision
Every committed chunk is a restart checkpoint: the reader persists its absolute logical
record position and the classification counters persist their absolute snapshot through
the ItemStream checkpoint path (SPEC 27, 37).

## Context
After a mid-file crash the same JobInstance must resume from the last checkpoint with no
double counting (SPEC 38; AC-008/009).

## Consequences
- Counters and the record anchor are absolute across executions — restored on restart and
  summed to the exact final totals.
- Not yet proven by tests: `WeeklyJobRestartTests` is a single end-to-end run over 10,000 records with exact
  reconciliation and no failure injection. Planned: failure-injection restart tests (fail at record 5,001, restart,
  reconcile), using the `RecordBoundaryFailureInjector` seam, which no test installs yet.
- Manifest transitions run in REQUIRES_NEW transactions and FAILED states are retryable,
  so a rolled-back step can never hide the terminal manifest state.
- Chunk-size tradeoff (checkpoint granularity vs DB metadata frequency vs memory) is
  configurable and benchmarked (docs/benchmarks).
