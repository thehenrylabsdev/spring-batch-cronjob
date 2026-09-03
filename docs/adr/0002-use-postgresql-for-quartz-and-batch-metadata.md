# ADR-0002: PostgreSQL for both Quartz and Spring Batch metadata

## Status
Accepted

## Decision
Single PostgreSQL instance hosting `quartz.QRTZ_*`, `batch.BATCH_*` (vendor 6.0.5 SQL) and
`control.*` (application-owned tables), separated into dedicated schemas (SPEC 16/17).
Flyway owns all DDL — framework auto-initialization is disabled (`initialize-schema: never`).

## Context
Persistence, clustering, restartability, transactional metadata and operational visibility
are core requirements (SPEC 15, 68). Schema separation prevents ownership confusion.

## Consequences
- One connection pool; `search_path = batch,quartz,control` (Hikari `connection-init-sql`)
  resolves unqualified framework names; Flyway keeps its history in the `batch` schema.
- Migrations run vendor PostgreSQL scripts verbatim, wrapped in `SET search_path`.
- H2 is never a substitute in tests — Testcontainers PostgreSQL everywhere (SPEC 61.3).
