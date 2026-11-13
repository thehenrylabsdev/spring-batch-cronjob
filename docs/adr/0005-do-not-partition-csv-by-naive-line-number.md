# ADR-0005: Do not partition CSV by naive line number

## Status
Accepted

## Decision
Never split the CSV file by physical newline/byte offsets computed without a logical
record pass. Any future partitioning must use precomputed safe record-boundary offsets
(one indexing pass) or upstream shards (SPEC 8.2).

## Context
Quoted fields can contain newlines (`1,"hello\nworld",100`); byte-offset arithmetic can
corrupt records and multiple rescanning readers duplicate I/O (SPEC 7, Anti-patterns 6/7).

## Consequences
- The distributed plane follows the rule above: the coordinator indexes safe logical-record boundaries once
  (ADR-0011), and workers seek to a committed byte checkpoint. The earlier parse-and-discard restart anchor belongs to
  the unwired single-reader step.
- `WeeklyJobRestartTests` runs a 10K-record end-to-end job; it does not inject a failure, so restart after a crash is
  not yet tested.
- The property seam `batch.processing.partitioning.mode` (SINGLE_READER | SAFE_BOUNDARIES | SHARDS) is bound but not
  read by any code.
