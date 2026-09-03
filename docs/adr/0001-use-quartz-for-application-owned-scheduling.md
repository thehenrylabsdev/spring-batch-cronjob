# ADR-0001: Use Quartz for application-owned scheduling

## Status
Accepted

## Decision
The weekly launch is driven by Quartz (clustered JDBC JobStore, `@DisallowConcurrentExecution`,
Configurable cron + zone + misfire policy), never by Kubernetes CronJob or `@Scheduled`.

## Context
- Persistent scheduler state, misfire behavior and clustered failover are required (FR-003..005).
- Schedule management should be application-owned (SPEC 55).

## Consequences
- Two pods share one QRTZ JobStore; exactly one fires each occurrence (verified by
  `QuartzClusterFiringTests`).
- Quartz only decides *when*; `WeeklyCsvQuartzJob` resolves, launches and returns (FR-002) —
  the batch runtime owns all record processing.
- Kubernetes CronJob is documented as an alternative (SPEC 55) and must not be configured
  simultaneously.
