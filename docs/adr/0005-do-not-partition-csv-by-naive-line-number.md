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
- Restart uses the logical-record anchor (parse-and-discard from the top of the file to
  the checkpoint) — no fragmentation risk; verified by the 10K restart test.
- The property seam `batch.processing.partitioning.mode` (SINGLE_READER | SAFE_BOUNDARIES |
  SHARDS) exists so a later switch does not redesign.
