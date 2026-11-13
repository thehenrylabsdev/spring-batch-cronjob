# ADR-0003: Single sequential reader for v1

## Status
Accepted. Implementation note: the single-reader `processCsvStep` is not wired into `weeklyCsvCoordinatorJob`; the
running job uses the distributed plane (ADR-0010, ADR-0011). See README "Implementation status".

## Decision
Level-1 architecture (SPEC 8): one sequential CSV reader feeding a bounded pool of chunk
workers. No naive line-range partitioning.

## Context
- Physical newlines are not logical record boundaries (quoted multiline fields), and
  multiple readers rescanning prefixes duplicate I/O (SPEC 7).
- Sequential file I/O is efficient; the measured 10M run (18 s) proves the single-reader
  path is far from the bottleneck.

## Consequences
- One stream, one parser instance, logical-record restart anchor (parse-and-discard skip
  on resume — no byte-offset arithmetic that could fragment records, ADR-0005/0007).
- End-to-end correctness: `WeeklyJobRestartTests` (one run, 10,000 records, exact counts) passes on the distributed
  path. No test exercises concurrent async chunks.
- Revisit only with measurement (ADR-0009).
