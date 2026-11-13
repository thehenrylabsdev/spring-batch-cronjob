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
- Byte-range partitioning does ship, in the distributed plane (ADR-0010, ADR-0011). That is more than the
  single-reader Level 1 this ADR describes, and the benchmark gate it calls for is not documented in the repository.
- The properties `batch.processing.partitioning.mode` and `grid-size` are bound but not read by any code. SHARDS
  (`MultiResourcePartitioner`-style) is not implemented.
- Remote partitioning is explicitly premature until production measurements contradict
  the baseline (SPEC 9: "measure and design carefully").
