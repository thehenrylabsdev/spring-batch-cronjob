# ADR-0006: Deterministic file identity

## Status
Accepted

## Decision
Job identity = `(businessDate, fileChecksum(SHA-256), fileUri)` carried as String job
parameters (SPEC 11). The checksum is computed exactly once at the ingestion boundary
(the launcher, before job launch) — never inside the processing job (SPEC 19).

## Context
The same logical file must map to the same JobInstance; completed/running instances must
not be silently re-launched; failed instances stay restartable; the manifest uniqueness
constraint `(business_date, checksum_sha256)` is the third serialization layer (SPEC 39).

## Consequences
- Duplicate launch outcomes: DUPLICATE_MANIFEST / DUPLICATE_RUNNING / DUPLICATE_COMPLETED
  — each increments `batch_duplicate_launch_prevented_total`.
- `batch.input.expected-checksum` is verified, not trusted: a tampered file cannot sail
  under a producer-claimed digest.
- The immutable copy is content-addressed (`customers-<sha256>.csv`, ADR on immutability
  in SPEC 14 strategy 2), so re-deliveries are idempotent.
