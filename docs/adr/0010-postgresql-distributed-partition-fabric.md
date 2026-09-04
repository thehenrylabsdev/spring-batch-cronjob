# ADR-0010: PostgreSQL is the distributed partition fabric

## Status

Accepted

## Decision

Workers register and heartbeat in `control.batch_worker`; CSV byte ranges reside in `control.batch_partition`.
Claims use `FOR UPDATE SKIP LOCKED`, a finite lease, and an incrementing fencing token. PostgreSQL—not Kubernetes
replica counts or an external broker—is the coordination source of truth.

## Consequences

- Workers can join and steal remaining ranges after the coordinator has planned them.
- An expired owner cannot checkpoint or complete after a newer lease version exists.
- Uncommitted chunks are at-least-once computation; committed checkpoints are durable.
