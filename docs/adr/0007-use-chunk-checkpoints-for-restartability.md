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
  summed to the exact final totals; `WeeklyJobRestartTests` (fail at record 5,001 of
  10,000, restart, exact reconciliation) is the automated proof.
- Manifest transitions run in REQUIRES_NEW transactions and FAILED states are retryable,
  so a rolled-back step can never hide the terminal manifest state.
- Chunk-size tradeoff (checkpoint granularity vs DB metadata frequency vs memory) is
  configurable and benchmarked (docs/benchmarks).
