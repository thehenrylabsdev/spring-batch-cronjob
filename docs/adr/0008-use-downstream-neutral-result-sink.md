# ADR-0008: Downstream-neutral result sink

## Status
Accepted

## Decision
The processing pipeline ends at `ProcessingResultSink`; v1 ships `ClassificationMetricsWriter`
(restart-safe counters, no broker). A future Kafka/Rabbit/Pulsar writer replaces/decorates
the sink without touching reader, processor, checkpoint model or identity (SPEC 75).

## Context
No message broker in this version; the architecture must preserve a clean extension seam.

## Consequences
- No broker-specific type appears in domain-processing APIs (checked by design);
  `ProcessingResultSink` accepts only `Chunk<? extends ProcessingResult>`.
- Counters + reconciliation + summary are the v1 sink behavior; they remain valid even
  when a broker sink is added (the broker sink would be invoked per chunk, not per record).
