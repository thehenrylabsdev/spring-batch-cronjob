# Distributed weekly CSV processing with Spring Batch, Quartz, Kubernetes, and PostgreSQL

This repository is a hands-on knowledge base and reference implementation for a horizontally scaled weekly CSV
cronjob. It explores how one immutable, potentially large CSV file can be processed cooperatively by **1–20
identical Spring Boot pods** without Kafka, Redis, or a separate coordination system.

The central idea is deliberately simple:

> Quartz decides which pod may start the weekly coordinator. PostgreSQL decides which worker may process each
> partition.

The project uses Java 25, Spring Boot 4, Spring Batch 6, Quartz JDBC JobStore, PostgreSQL, Flyway, Kubernetes,
Micrometer/Actuator, and Testcontainers.

## Why this exists

A common misconception is that deploying more replicas of a scheduled Spring Batch application automatically
distributes its CSV records. It does not. Quartz clustering prevents duplicate trigger firing, but it does not turn
a single Spring Batch reader into distributed work.

This project documents and implements the missing distributed work layer:

```text
Quartz cluster       -> exactly one coordinator launch
PostgreSQL workers   -> discovery through heartbeats
CSV boundary index   -> safe random-access byte ranges
PostgreSQL queue     -> SKIP LOCKED claims and work stealing
Lease + fence token  -> safe crash recovery
Chunk checkpoint     -> resume from committed byte offset
Reconciliation       -> prove every logical record was covered
```

## Architecture

```text
                         PostgreSQL
  +----------------------------------------------------------+
  | quartz.QRTZ_*       clustered Quartz schedule            |
  | batch.BATCH_*       Spring Batch execution metadata      |
  | control.*           manifests, workers, partitions, runs |
  +-------------------------------+--------------------------+
                                  ^
                                  |
       +--------------------------+-------------------------+
       |                          |                         |
  Spring Boot pod 1          Spring Boot pod 2          Spring Boot pod N
  Quartz + worker            Quartz + worker            Quartz + worker
       |                          |                         |
       +--- register / heartbeat / claim durable work ---+---+
                                  |
                  Monday Quartz trigger fires once
                                  |
                                  v
                       coordinator indexes CSV once
                                  |
                                  v
               safe byte-range partitions become READY
                                  |
                                  v
                    workers claim -> process -> checkpoint
                                  |
                                  v
                     reconcile ranges and record totals
```

## Key properties

- A CSV record can contain quoted multiline fields. Partitions are never formed by physical line number or arbitrary
  byte position.
- The coordinator scans the file once with a quote-aware UTF-8 boundary state machine. It computes SHA-256, validates
  boundaries, finds the header end, counts logical records, and retains only partition boundaries.
- Workers seek straight to a committed byte checkpoint. They never rescan the file prefix or skip millions of records.
- PostgreSQL `FOR UPDATE SKIP LOCKED` provides concurrent, short-lived work claims.
- Each claim increments a `lease_version` fencing token. A stale worker cannot checkpoint or complete a partition after
  a newer owner claims it.
- Uncommitted chunks may be computed again after failure. This is **at-least-once computation at the chunk boundary**,
  not distributed exactly-once side effects.
- Workers are discovered through fresh PostgreSQL heartbeats, never by Kubernetes replica count.
- Partitions are over-created and dynamically claimed, so fast pods steal more work and new pods can join mid-run.

## Implementation status

What the code does today, checked against the source. The specs (`SPEC.md`, `SPEC_02.md`) describe the target.

Implemented:

- Quartz clustered weekly trigger launches `weeklyCsvCoordinatorJob` once per occurrence.
- Coordinator validates the input, loads reference data, runs one quote-aware UTF-8 indexing pass (SHA-256, header end,
  record count, byte-range partitions) and persists them in `control.batch_partition`.
- Each pod runs one worker polling loop per slot (`batch.distributed.worker.concurrency`, default `1`). Claims use
  `FOR UPDATE SKIP LOCKED`, fenced by `lease_version`; expired leases are requeued.
- Chunk checkpoints persist the byte offset and counters; a crashed partition resumes from its last committed checkpoint.
- Input checks: size on workers; size and modification time on the coordinator before and after indexing.
- `WeeklyJobRestartTests` runs one end-to-end distributed job over 10,000 generated records and asserts exact counts. It
  does not inject a failure and does not restart a job.

Planned, not yet implemented:

- Graceful shutdown drain-and-release. Today a stopping pod marks itself `DRAINING` in `control.batch_worker`, but nothing
  reads that flag, it does not release its partition after a checkpoint, and it keeps processing its in-flight chunk until the
  JVM exits. Its unfinished range is recovered only by lease expiry.
- Worker-side modification-time check. Workers compare file size only.
- Failure-injection restart tests. The `RecordBoundaryFailureInjector` seam exists, but no test installs it.
- `batch.processing.executor.*` drives `batchProcessingExecutor`, which exposes gauges, but no code submits work to it. The
  distributed workers do not use it.

Bound but not read by any code (reserved; changing them has no effect):

- `batch.distributed.partition.lease-heartbeat`. Leases renew only at chunk checkpoints, so a single chunk slower than
  `lease-duration` can lose its lease.
- `batch.processing.partitioning.mode` and `grid-size`. Byte-range partitioning is always on.
- `batch.processing.skip-limit`. No Spring Batch skip policy is attached; invalid rows are counted as `INVALID`.

## Stack

| Concern | Technology |
| --- | --- |
| Language / runtime | Java 25 |
| Application | Spring Boot 4.x |
| Batch orchestration | Spring Batch 6.x |
| Weekly schedule | Quartz with JDBC JobStore |
| Coordination / metadata | PostgreSQL |
| Schema ownership | Flyway |
| Deployment target | Kubernetes |
| Metrics and health | Spring Boot Actuator + Micrometer Prometheus |
| Integration testing | Testcontainers PostgreSQL |
| CSV parser | Univocity, with a raw byte boundary indexer |

## Prerequisites

- JDK 25
- Docker Desktop or another Docker-compatible runtime for Testcontainers and local PostgreSQL
- PostgreSQL 16+ for a deployed environment
- Maven Wrapper included in this repository
- Kubernetes cluster and a shared read-only volume for multi-pod production execution

## Run locally

Start PostgreSQL with the repository's compose configuration:

```bash
docker compose up -d
./mvnw spring-boot:run
```

The development default is `classpath:/batch/weekly.csv`. It is materialized to a seekable temporary file on each pod,
which makes byte-range processing work even when the resource is packaged inside the application JAR.

Run the tests:

```bash
./mvnw test
```

Run formatting and compilation checks:

```bash
./mvnw spring-javaformat:apply
./mvnw -DskipTests compile
```

## Production input contract

Classpath input is for development and testing only. A production weekly file must be provided through a shared,
read-only filesystem visible to every worker pod:

```text
BATCH_INPUT_RESOURCE=file:/data/weekly/weekly.csv
```

The upstream publisher writes an immutable Sunday generation and atomically publishes its final name:

```text
weekly.csv.uploading -> write + close -> atomic rename -> weekly.csv
```

The final file must not change while Monday processing runs. The coordinator checks size and modification time before
and after indexing. Workers recheck the file size only when beginning and completing work; a worker-side modification
time check is planned (see Implementation status).

## Important configuration

```yaml
batch:
  schedule:
    cron: "0 0 6 ? * MON"
    zone: "Asia/Ho_Chi_Minh"

  input:
    resource: ${BATCH_INPUT_RESOURCE:classpath:/batch/weekly.csv}
    stability-window: 10m

  processing:
    chunk-size: 2000

  distributed:
    enabled: true
    worker:
      concurrency: 1
      heartbeat-interval: 5s
      active-timeout: 20s
      idle-poll-interval: 1s
    partition:
      partitions-per-active-worker: 8
      max-pods: 20
      scale-headroom-partitions-per-max-pod: 2
      max-partitions: 320
      min-partition-bytes: 32MB
      lease-duration: 30s
      lease-heartbeat: 10s         # reserved: bound but not read (see Implementation status)
      max-attempts: 3
    no-worker-timeout: 5m
```

For the initial production deployment, keep `worker.concurrency=1`. If raising it after benchmarking, size CPU,
memory, PostgreSQL connections, and the worker-slot advertisement together.

## Distributed lifecycle

1. Each pod derives `worker_id` from Kubernetes `POD_UID` (or a startup UUID outside Kubernetes).
2. Each pod inserts/updates `control.batch_worker` and sends heartbeats.
3. Exactly one Quartz node acquires the stable weekly trigger and launches `weeklyCsvCoordinatorJob`.
4. The coordinator validates the input, snapshots active worker slots, and performs one CSV-aware indexing pass.
5. It persists contiguous `control.batch_partition` rows in `READY` state.
6. Any healthy worker claims the next row with `FOR UPDATE SKIP LOCKED`.
7. The worker runs chunked processing, persists the safe next byte offset and aggregate counters, and renews its lease.
8. Planned: a gracefully terminating pod stops claiming new work and releases an unfinished partition after a checkpoint.
   Today it does not release; its partition is reclaimed after lease expiry.
9. An abruptly lost pod stops heartbeating; its lease expires and another pod resumes from the last committed checkpoint.
10. The manager and reconciliation steps require all ranges to complete with matching expected counts before the manifest
    becomes `COMPLETED`.

## PostgreSQL schemas

| Schema | Ownership |
| --- | --- |
| `quartz` | Quartz `QRTZ_*` scheduler state |
| `batch` | Spring Batch `BATCH_*` metadata |
| `control` | File manifests, workers, partitions, run summaries |

Flyway owns the schema. Application runtime users need DML privileges only; production DDL privileges are not needed.

## Kubernetes

`k8s/deployment.yaml` demonstrates:

- non-root, read-only container filesystem;
- Downward API injection of `POD_NAME`, `POD_UID`, and `POD_NAMESPACE`;
- a shared read-only weekly input mount at `/data/weekly`;
- graceful termination period for an in-flight chunk;
- PostgreSQL readiness via Actuator probes.

All replicas must mount the **same file generation**, not separate local files that happen to have the same path.

## Operations

Actuator exposes health, metrics, Prometheus, and Quartz endpoints. The detailed operational SQL and recovery guidance
are in [the weekly batch runbook](docs/runbook/weekly-csv-batch.md).

Useful first queries:

```sql
SELECT worker_id, status, worker_slots, last_heartbeat_at, current_partition_id
FROM control.batch_worker
ORDER BY last_heartbeat_at DESC;

SELECT partition_no, status, worker_id, lease_version, checkpoint_record_no, expected_record_count
FROM control.batch_partition
WHERE manifest_id = :manifest_id
ORDER BY partition_no;
```

## Design notes and limitations

- This is a learning/reference project, not a claim of production certification.
- It intentionally has no message broker, distributed cache, Kubernetes API polling, or external lock service.
- It does not guarantee exactly-once effects to external systems. Any future external writer must be idempotent.
- A pre-indexing pass is a deliberate cost that enables safe random access and avoids repeated full-file scans.
- Benchmark with representative inputs and 1, 2, 5, 10, and 20 pods before setting capacity or SLOs.

More background is available in [SPEC_02.md](SPEC_02.md), [architecture decision records](docs/adr), and the
[legacy baseline](docs/legacy-baseline.md).

## License

This project is licensed under the [Apache License 2.0](LICENSE).
