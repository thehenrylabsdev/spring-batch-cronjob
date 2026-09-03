# ADR-0009: Scale by measurement before remote partitioning

## Status
Accepted

## Decision
Level 1 (single sequential reader + bounded concurrent chunks) is the shipped architecture.
Local (Level 2) and remote (Level 4) partitioning are gated on benchmark evidence, in the
order mandated by SPEC 62.

## Context
Measured baseline (docs/benchmarks/defaults.md): 10M representative records in ~18 s on
local development hardware — roughly 200x inside the 60-minute acceptance SLA. The
single-reader path is not the bottleneck; processing workers are not starved.

## Consequences
- No partitioning code ships beyond the property seam
  (`batch.processing.partitioning.mode`, `grid-size`; SHARDS resolves via
  `MultiResourcePartitioner`-style structure when upstream shards appear).
- Remote partitioning is explicitly premature until production measurements contradict
  the baseline (SPEC 9: "measure and design carefully").
