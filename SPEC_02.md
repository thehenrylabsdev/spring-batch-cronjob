# SPEC_02.md — Distributed Weekly CSV Processing for a Horizontally Scaled Legacy Spring Boot Application

> **Note:** This is the target specification. Statements about worker modification-time checks, graceful drain and
> release (sections on shutdown and drain), and the reserved `lease-heartbeat` and `skip-limit` properties are not
> implemented yet. The current state is listed in the README's "Implementation status" section.

## 1. Document Status

- **Status:** Implementation Specification
- **Purpose:** Upgrade an existing/legacy cronjob application so one weekly CSV workload can be processed cooperatively by 1–20 running application pods.
- **Primary audience:** AI coding agents, Solution Architects, Principal Engineers, Backend Engineers, SRE/DevOps Engineers
- **Language:** English
- **Target Java:** Java 25
- **Application framework:** Spring Boot 4.x
- **Batch framework:** Spring Batch 6.x
- **Scheduler:** Quartz Scheduler with JDBC JobStore
- **Database:** PostgreSQL only
- **Deployment target:** Kubernetes
- **Input:** One large CSV file, replaced/updated every Sunday and processed on Monday
- **Message broker:** None
- **External distributed cache:** None
- **External coordination service:** None

This specification is normative.

Keywords:

- **MUST / MUST NOT** — mandatory.
- **SHOULD / SHOULD NOT** — strongly recommended unless a documented reason exists.
- **MAY** — optional.

---

# 2. Executive Summary

The cronjob application will run as **1–20 identical Spring Boot pods**.

All pods will:

1. participate in one Quartz JDBC cluster,
2. register themselves as available batch workers in PostgreSQL,
3. send periodic heartbeats to PostgreSQL,
4. be able to claim and process CSV partitions,
5. share one Spring Batch `JobRepository`,
6. share one PostgreSQL-backed distributed coordination state.

At the Monday schedule:

1. Quartz allows only one clustered scheduler node to acquire the trigger.
2. That pod becomes the **job coordinator** for the current weekly file.
3. The coordinator detects the number of active worker pods from PostgreSQL.
4. The coordinator validates and indexes the CSV in one sequential pre-scan.
5. The coordinator creates many safe, non-overlapping CSV byte-range partitions.
6. All active pods dynamically claim partitions from PostgreSQL.
7. Workers process partitions independently.
8. Workers checkpoint progress at chunk boundaries.
9. If a worker disappears, its lease expires and another worker resumes its unfinished partition.
10. The coordinator reconciles all partition results before marking the file/job complete.

The system MUST NOT use:

```text
pod-1 -> first 1,000,000 rows
pod-2 -> skip 1,000,000 rows and process next 1,000,000
...
```

because that causes duplicated file scanning and is unsafe for CSV records containing quoted multiline fields.

Instead, the system will use a **CSV-aware byte boundary index**, a PostgreSQL work queue, leases, fencing tokens, and work stealing.

---

# 3. Core Architectural Decision

The key design is:

> **Scheduler scaling and workload scaling are different concerns.**

Quartz answers:

```text
WHO is allowed to launch the weekly job?
```

The distributed partition subsystem answers:

```text
WHICH pod processes WHICH part of the CSV?
```

Scaling the Kubernetes Deployment from 1 to 20 replicas MUST automatically increase the number of active workers available to the same weekly workload.

The system MUST NOT assume that increasing the Quartz replica count automatically distributes Spring Batch records.

---

# 4. Primary Goals

The upgraded system MUST:

1. Run the weekly job every Monday.
2. Safely process one very large CSV file.
3. Support 1–20 application pods.
4. Automatically discover how many pods are actually active.
5. Automatically distribute work among active pods.
6. Dynamically rebalance work if pods finish at different speeds.
7. Allow newly started pods to join an in-progress job.
8. Recover work when a pod is terminated or crashes.
9. Avoid duplicate ownership of a partition.
10. Preserve restart/checkpoint state.
11. Ensure every logical CSV record belongs to exactly one partition.
12. Ensure no logical CSV record is omitted.
13. Detect file replacement/modification during a job.
14. Keep memory usage bounded.
15. Avoid per-record PostgreSQL access.
16. Avoid one thread per CSV record.
17. Use PostgreSQL as the only distributed coordination dependency.
18. Preserve Spring Batch job metadata and restartability.
19. Preserve Quartz scheduling HA.
20. Provide production-grade metrics, logging, recovery, and reconciliation.
21. Allow a development CSV to live under `src/main/resources`.
22. Allow production file location to be provided through environment variables/application configuration.
23. Upgrade the legacy application incrementally and safely.

---

# 5. Non-Goals

The following are explicitly out of scope:

- Kafka.
- RabbitMQ.
- JMS.
- Pulsar.
- Redis.
- ZooKeeper.
- Hazelcast.
- Spring Cloud Data Flow.
- External distributed lock services.
- Real-time stream processing.
- Processing multiple weekly CSV files in parallel.
- Creating one partition per CSV record.
- Loading the entire CSV into memory.
- Kubernetes API polling as the primary mechanism for worker discovery.
- Exactly-once side effects to an external system.
- A user-facing web UI.

---

# 6. Important Constraint: One File, Many Pods

The application receives only one large CSV file.

The system MUST NOT distribute work by having every pod start at the beginning and skip records until it reaches its assigned range.

Bad design:

```text
10M rows

pod-1:
scan 0M -> 1M

pod-2:
scan 0M -> 2M

pod-3:
scan 0M -> 3M

...

pod-10:
scan 0M -> 10M
```

Approximate total scanning:

```text
1M + 2M + ... + 10M = 55M row reads
```

to process only 10M rows.

This is prohibited.

The implementation MUST provide true/random-access partition start offsets.

---

# 7. CSV Correctness Constraint

The implementation MUST assume a CSV logical record can span multiple physical lines unless the business contract explicitly forbids it.

Example:

```csv
1001,"Henry","Line 1
Line 2","ACTIVE"
```

Therefore:

```text
physical line number != logical CSV record number
```

The system MUST NOT split the file by arbitrary newline or arbitrary byte position.

Partition boundaries MUST always occur at validated **logical CSV record boundaries**.

---

# 8. High-Level Architecture

```text
                                      PostgreSQL
                 +------------------------------------------------+
                 | quartz.QRTZ_*                                  |
                 | batch.BATCH_*                                  |
                 | control.batch_file_manifest                    |
                 | control.batch_worker                           |
                 | control.batch_partition                        |
                 | control.batch_run_summary                      |
                 +------------------------+-----------------------+
                                          ^
                                          |
               +--------------------------+--------------------------+
               |                          |                          |
         cronjob-app-1               cronjob-app-2              cronjob-app-N
         Spring Boot                 Spring Boot                Spring Boot
         Quartz                      Quartz                     Quartz
         Worker                      Worker                     Worker
               |                          |                          |
               +--------------------------+--------------------------+
                                          |
                             all register + heartbeat
                                          |
                                          v
                              Active Worker Registry
                                          |
                               Monday Quartz Trigger
                                          |
                                  one pod acquires
                                          |
                                          v
                                   Job Coordinator
                                          |
                         validate + index one CSV file
                                          |
                                          v
                             create safe partitions
                                          |
                          +---------------+---------------+
                          |               |               |
                          v               v               v
                       READY           READY           READY
                       P000            P001            Pxxx
                          |               |               |
                          +------- PostgreSQL queue ------+
                                          |
                    +---------------------+----------------------+
                    |                     |                      |
                    v                     v                      v
                  pod-1                 pod-2                  pod-N
                  claim                 claim                  claim
                  P000                  P001                   P00N
                    |                     |                      |
                    v                     v                      v
            byte-range reader     byte-range reader      byte-range reader
                    |                     |                      |
                    v                     v                      v
               processor              processor               processor
                    |                     |                      |
                    v                     v                      v
              checkpoint             checkpoint              checkpoint
                    |                     |                      |
                    +---------------------+----------------------+
                                          |
                                          v
                                      reconcile
                                          |
                                          v
                                      COMPLETED
```

---

# 9. Pod Roles

All application pods run the same artifact.

Every pod MAY perform three logical roles:

## 9.1 Quartz Scheduler Node

All pods join the same Quartz JDBC cluster.

## 9.2 Coordinator

Only the pod that acquires the Monday Quartz trigger starts the weekly coordinator job.

## 9.3 Worker

All healthy pods participate in partition processing.

The coordinator pod MAY also process partitions.

No dedicated coordinator Deployment is required for V1.

---

# 10. Why Worker Discovery Uses PostgreSQL

The system MUST NOT depend on querying the Kubernetes Deployment replica count.

Reasons:

1. desired replicas are not equal to healthy/ready workers,
2. pods can be starting,
3. pods can be terminating,
4. pods can be temporarily unhealthy,
5. a desired replica may not be schedulable,
6. the implementation should remain portable outside Kubernetes,
7. PostgreSQL is already the shared system of record.

Instead:

```text
running pod
    ->
register worker
    ->
heartbeat PostgreSQL
    ->
coordinator counts fresh heartbeats
```

This gives the coordinator the number of **actually participating workers**.

Kubernetes Downward API MAY provide stable pod metadata such as:

```text
POD_NAME
POD_UID
POD_NAMESPACE
```

but PostgreSQL remains the worker-discovery source of truth.

---

# 11. Worker Identity

In Kubernetes, worker identity SHOULD use Pod UID.

Recommended environment variables:

```text
POD_NAME
POD_UID
POD_NAMESPACE
```

Example Deployment configuration:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name

  - name: POD_UID
    valueFrom:
      fieldRef:
        fieldPath: metadata.uid

  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
```

Worker ID:

```text
workerId = POD_UID
```

Fallback outside Kubernetes:

```text
workerId = UUID generated at application startup
```

---

# 12. Worker Registry

Create:

```text
control.batch_worker
```

Recommended columns:

```sql
worker_id              varchar primary key
pod_name               varchar
pod_namespace          varchar
application_version    varchar
status                 varchar
worker_slots           integer
started_at             timestamptz
last_heartbeat_at      timestamptz
current_job_execution  bigint null
current_partition_id   bigint null
shutdown_requested     boolean
created_at             timestamptz
updated_at             timestamptz
```

Worker statuses:

```text
STARTING
IDLE
BUSY
DRAINING
STOPPED
```

Recommended indexes:

```text
(status, last_heartbeat_at)
(current_job_execution)
```

---

# 13. Worker Heartbeat

Default configuration:

```yaml
batch:
  distributed:
    worker:
      heartbeat-interval: 5s
      active-timeout: 20s
```

Each worker MUST update:

```text
last_heartbeat_at
status
current_job_execution
current_partition_id
```

at least once per heartbeat interval.

A worker is considered active when:

```text
status IN ('IDLE', 'BUSY')
AND
last_heartbeat_at >= now() - active_timeout
```

The coordinator MUST use this query rather than Kubernetes replica count.

---

# 14. Worker Slots

Default:

```text
1 worker slot per pod
```

Therefore:

```text
10 active pods = 10 active worker slots
```

Later, a pod MAY expose more than one slot:

```yaml
batch:
  distributed:
    worker:
      concurrency: 1
```

The coordinator SHOULD calculate:

```text
activeWorkerSlots = SUM(worker_slots)
```

instead of assuming one forever.

For the initial legacy upgrade:

```text
worker_slots = 1
```

is mandatory unless performance testing justifies local concurrency.

---

# 15. Quartz Cluster

Production MUST use Quartz JDBC JobStore.

All application pods MUST share the same PostgreSQL Quartz schema.

Conceptual configuration:

```yaml
spring:
  quartz:
    job-store-type: jdbc
    properties:
      org.quartz.scheduler.instanceId: AUTO
      org.quartz.jobStore.isClustered: true
```

Exact property names MUST match the Spring Boot/Quartz version selected by dependency management.

---

# 16. Quartz Job Identity

Use one stable `JobKey`.

Example:

```text
group = weekly-batch
name  = weekly-csv-coordinator
```

The identity MUST NOT contain:

- pod name,
- random UUID,
- application instance ID.

All pods MUST register the same logical Quartz JobDetail.

---

# 17. Quartz Concurrent Execution Protection

The Quartz launcher job MUST use:

```java
@DisallowConcurrentExecution
```

or the equivalent supported mechanism.

This prevents overlapping execution of the same Quartz `JobKey` across the clustered scheduler.

This is only scheduler-level protection.

Spring Batch job identity and database uniqueness constraints provide additional protection.

---

# 18. Monday Schedule

Default:

```yaml
batch:
  schedule:
    cron: "0 0 6 ? * MON"
    zone: "Asia/Ho_Chi_Minh"
```

Both MUST be configurable.

The application MUST NOT rely on container default timezone.

---

# 19. Quartz Misfire Policy

Default behavior:

> If the Monday trigger was missed while the entire scheduler cluster was unavailable, fire the weekly job once when the scheduler becomes available.

Default semantic:

```text
FIRE_ONCE_NOW
```

This MUST be explicit.

---

# 20. Spring Batch Job Identity

Recommended job:

```text
weeklyCsvCoordinatorJob
```

Identifying parameters:

```text
businessDate
fileLogicalName
fileGeneration
```

After the coordinator computes the file SHA-256, the manifest MUST be associated with:

```text
sha256
fileSize
lastModified
```

The system MUST prevent multiple completed executions for the same logical file generation.

---

# 21. Spring Batch Metadata

Production MUST use the JDBC `JobRepository`.

PostgreSQL is required.

Use separate schemas:

```text
quartz
batch
control
```

Responsibilities:

```text
quartz.QRTZ_*
    Quartz scheduler persistence

batch.BATCH_*
    Spring Batch Job/Step execution metadata

control.*
    distributed worker/partition/file state
```

---

# 22. Production Schema Initialization

Production MUST use Flyway or the existing project migration tool.

The application MUST NOT rely on automatic destructive framework schema initialization.

Suggested migrations:

```text
Vxxx__create_quartz_schema.sql
Vxxx__create_quartz_tables.sql

Vxxx__create_batch_schema.sql
Vxxx__create_spring_batch_tables.sql

Vxxx__create_control_schema.sql
Vxxx__create_batch_file_manifest.sql
Vxxx__create_batch_worker.sql
Vxxx__create_batch_partition.sql
Vxxx__create_batch_run_summary.sql
```

Framework table DDL MUST come from the official scripts matching the resolved framework versions.

---

# 23. Input Resource Configuration

Use one configuration abstraction:

```yaml
batch:
  input:
    resource: ${BATCH_INPUT_RESOURCE:classpath:/batch/weekly.csv}
```

## Development

Default:

```text
classpath:/batch/weekly.csv
```

File location:

```text
src/main/resources/batch/weekly.csv
```

## Production

Example:

```text
BATCH_INPUT_RESOURCE=file:/data/weekly/weekly.csv
```

or:

```yaml
batch:
  input:
    resource: ${BATCH_INPUT_RESOURCE}
```

---

# 24. Critical Classpath Limitation

A file in:

```text
src/main/resources
```

is packaged into the application JAR/container image.

Therefore classpath mode is **development/testing only**.

A Sunday file cannot be safely replaced inside an already-built production JAR.

Production MUST use an external filesystem resource.

---

# 25. Production Shared-File Requirement

When more than one pod processes the same file, every worker MUST see the same file bytes.

Therefore the production path MUST resolve to a shared mount, for example:

```text
/data/weekly/weekly.csv
```

mounted into all pods from a shared persistent volume or enterprise file share.

The following is invalid:

```text
pod-1 /data/weekly.csv -> local file A
pod-2 /data/weekly.csv -> local file B
```

even if the path string is identical.

All worker pods MUST resolve the same file generation.

---

# 26. Development Resource Materialization

Distributed byte-range processing requires seekable file access.

A classpath resource inside an executable Spring Boot JAR is not guaranteed to be directly seekable as a normal filesystem file.

Therefore development mode MUST materialize the classpath CSV to a local temporary file before partition processing.

Example:

```text
classpath:/batch/weekly.csv
        |
        v
copy once per pod
        |
        v
${java.io.tmpdir}/cronjob-batch/<generation>/weekly.csv
```

Each pod then performs byte-range reads against its own identical local materialized copy.

All pods MUST verify the expected:

```text
file size
generation identity
```

before processing a partition.

---

# 27. Production File Update Contract

Sunday file publishing MUST follow an immutable-generation contract.

Recommended upstream workflow:

```text
weekly.csv.uploading
        |
write complete file
fsync/close
        |
        v
atomic rename
        |
        v
weekly.csv
```

Once `weekly.csv` becomes visible as the final Sunday generation:

```text
it MUST NOT be modified during Monday processing
```

A new weekly generation should replace it only after the previous week's job is finished.

---

# 28. File Stability Check

Configuration:

```yaml
batch:
  input:
    stability-window: 10m
```

Before processing, the coordinator MUST verify:

```text
now - lastModified >= stabilityWindow
```

It MUST also capture:

```text
size before index scan
lastModified before index scan
```

and compare them with:

```text
size after index scan
lastModified after index scan
```

If either changed:

```text
FAIL job
```

The system MUST NOT process a file that changes during indexing.

---

# 29. File Manifest

Create:

```text
control.batch_file_manifest
```

Recommended columns:

```sql
id                       bigint generated always as identity primary key
business_date            date not null
logical_name             varchar not null
resource_location        varchar not null
file_size_bytes          bigint
last_modified_at         timestamptz
sha256                   varchar
header_hash              varchar
expected_record_count    bigint
status                   varchar not null
index_started_at         timestamptz
index_finished_at        timestamptz
processing_started_at    timestamptz
processing_finished_at   timestamptz
created_at               timestamptz not null
updated_at               timestamptz not null
```

Statuses:

```text
DISCOVERED
VALIDATING
INDEXING
READY
PROCESSING
COMPLETED
FAILED
QUARANTINED
```

Recommended uniqueness after SHA-256 is known:

```text
UNIQUE(logical_name, sha256)
```

---

# 30. Why a Pre-Index Pass Is Required

The system needs safe byte offsets so workers can seek directly to their partition.

The coordinator performs one sequential **CSV boundary indexing pass**.

This pass MUST simultaneously:

1. validate CSV record boundaries,
2. identify header end offset,
3. calculate file SHA-256,
4. count logical data records,
5. create approximately equal-sized safe partition boundaries,
6. detect malformed/unclosed quoted records,
7. validate the file did not change during the scan.

This adds one full sequential read before distributed processing.

That is an intentional correctness/performance tradeoff.

It avoids:

- repeated scans by every worker,
- corrupt CSV boundaries,
- unsafe line-number skipping.

---

# 31. CSV Encoding Contract

V1 SHOULD require:

```text
UTF-8
```

Config:

```yaml
batch:
  input:
    encoding: UTF-8
    delimiter: ","
    quote: '"'
```

The boundary indexer MUST understand:

- LF,
- CRLF,
- quoted fields,
- escaped/doubled quotes,
- quoted multiline fields.

If the legacy CSV contract is simpler, that MUST be documented but the implementation SHOULD remain RFC-4180-compatible where practical.

---

# 32. CSV Boundary Indexer

Implement:

```java
public interface CsvBoundaryIndexer {
    CsvFileIndex index(SeekableByteChannel channel, PartitionPlan plan);
}
```

The implementation SHOULD scan raw bytes because:

- byte offsets are required,
- UTF-8 uses the same single-byte ASCII representations for comma, quote, CR, and LF,
- logical record boundaries can therefore be located safely using a quote-aware state machine.

Do not use:

```text
BufferedReader line count
```

as the partitioning source of truth when multiline CSV is supported.

---

# 33. Boundary State Machine

At minimum, the indexer MUST distinguish:

```text
OUTSIDE_QUOTED_FIELD
INSIDE_QUOTED_FIELD
AFTER_QUOTE
```

A newline terminates a logical record only when not inside a quoted field.

Escaped quote:

```csv
"Henry ""H"" Nguyen"
```

MUST NOT close the field at the first quote.

All state-machine behavior MUST be covered by unit tests.

---

# 34. Active Worker Snapshot

At indexing/partition planning time:

```text
W = number of active worker slots
```

The coordinator MUST store:

```text
activeWorkerCountAtPlan
activeWorkerSlotsAtPlan
```

for observability.

However, the partition allocation MUST remain dynamic after planning.

New workers MAY join later.

Existing workers MAY disappear.

---

# 35. Over-Partitioning

The system MUST NOT create exactly one partition per pod.

Bad:

```text
10 pods -> exactly 10 partitions
```

because a slow partition can determine total job duration.

Instead use over-partitioning.

Default:

```yaml
batch:
  distributed:
    partition:
      partitions-per-active-worker: 8
      scale-headroom-partitions-per-max-pod: 2
      max-pods: 20
      max-partitions: 320
      min-partition-bytes: 32MB
```

Conceptual formula:

```text
activeTarget =
    activeWorkerSlots * partitionsPerActiveWorker

scaleHeadroom =
    maxPods * scaleHeadroomPartitionsPerMaxPod

candidate =
    max(activeTarget, scaleHeadroom)

partitionCount =
    clamp(candidate, 1, maxPartitions)
```

Then reduce partition count if the file would produce partitions smaller than:

```text
minPartitionBytes
```

This design allows a job planned when only a few pods are running to still benefit if more pods join later.

---

# 36. Example Partition Counts

Example defaults:

```text
partitionsPerActiveWorker = 8
maxPods                   = 20
scaleHeadroom             = 40
maxPartitions             = 320
```

If active pods at planning time:

```text
1 pod
activeTarget = 8
candidate    = max(8, 40)
             = 40 partitions

5 pods
activeTarget = 40
candidate    = 40 partitions

10 pods
activeTarget = 80
candidate    = 80 partitions

20 pods
activeTarget = 160
candidate    = 160 partitions
```

Actual partition count can be reduced for a small file.

---

# 37. Why Not Exactly Divide 10M / Pod Count

The desired behavior is not:

```text
10 pods
=> exactly 1,000,000 records per pod
```

The desired behavior is:

```text
many balanced partitions
+
dynamic claiming
+
work stealing
```

Example:

```text
10 pods
80 partitions
~125K records average per partition
```

A fast pod can finish several partitions.

A slow pod processes fewer.

This minimizes the straggler problem.

---

# 38. Partition Boundaries

For target partition count `P` and file size `B`:

```text
targetByteSize ~= B / P
```

During the sequential CSV-aware scan:

1. track current safe logical-record boundary,
2. when the current offset passes the next target byte threshold,
3. close the current partition at that safe boundary,
4. start the next partition there.

This creates approximately equal byte-size partitions without splitting a record.

---

# 39. Partition Coverage Invariant

After indexing:

```text
P0.start = dataStartOffset

Pi.endExclusive = P(i+1).start

P(last).endExclusive = fileSize
```

Required invariant:

```text
union(all partitions)
=
all CSV data bytes after header
```

Required properties:

```text
no gaps
no overlaps
ordered ranges
all boundaries are logical record boundaries
```

The coordinator MUST validate these invariants before changing the manifest to `READY`.

---

# 40. Partition Expected Record Counts

The index pass MUST count logical records.

Each partition MUST store:

```text
startRecordNumber
endRecordNumberExclusive
expectedRecordCount
```

Final worker result MUST equal the expected count unless records are explicitly categorized as parser-invalid according to the configured policy.

---

# 41. Partition Table

Create:

```text
control.batch_partition
```

Recommended columns:

```sql
id                         bigint generated always as identity primary key
manifest_id                bigint not null
job_execution_id           bigint not null
partition_no               integer not null

start_byte                 bigint not null
end_byte_exclusive         bigint not null

start_record_no            bigint not null
end_record_no_exclusive    bigint not null
expected_record_count      bigint not null

status                     varchar not null

worker_id                  varchar null
lease_version              bigint not null default 0
lease_expires_at           timestamptz null

checkpoint_byte            bigint not null
checkpoint_record_no       bigint not null

committed_read_count       bigint not null default 0
committed_processed_count  bigint not null default 0
accepted_count             bigint not null default 0
rejected_count             bigint not null default 0
invalid_count              bigint not null default 0

attempt_count              integer not null default 0
last_error_code            varchar null
last_error_message         varchar null

claimed_at                 timestamptz null
completed_at               timestamptz null
created_at                 timestamptz not null
updated_at                 timestamptz not null
```

Unique constraint:

```text
UNIQUE(manifest_id, partition_no)
```

Indexes:

```text
(job_execution_id, status, partition_no)
(status, lease_expires_at)
(worker_id, status)
```

---

# 42. Partition Status State Machine

Allowed states:

```text
READY
CLAIMED
RUNNING
COMPLETED
FAILED
RETRYABLE
CANCELLED
```

Typical lifecycle:

```text
READY
  |
  v
CLAIMED
  |
  v
RUNNING
  |
  +------> RETRYABLE ------+
  |                        |
  |                        v
  |                      CLAIMED
  |
  v
COMPLETED
```

---

# 43. PostgreSQL Work Claiming

Workers MUST claim work atomically.

Use PostgreSQL row locking:

```sql
FOR UPDATE SKIP LOCKED
```

Conceptual claim transaction:

```sql
WITH candidate AS (
    SELECT id
    FROM control.batch_partition
    WHERE job_execution_id = :jobExecutionId
      AND status IN ('READY', 'RETRYABLE')
    ORDER BY partition_no
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE control.batch_partition p
SET status           = 'CLAIMED',
    worker_id        = :workerId,
    lease_version    = p.lease_version + 1,
    lease_expires_at = now() + :leaseDuration,
    claimed_at       = now(),
    attempt_count    = p.attempt_count + 1,
    updated_at       = now()
FROM candidate
WHERE p.id = candidate.id
RETURNING p.*;
```

The transaction MUST be short.

It MUST NOT remain open while the partition is processed.

---

# 44. Why `SKIP LOCKED`

With multiple workers all claiming concurrently, a row already locked by another short claim transaction is skipped.

This provides a database-backed queue-like claim mechanism without a message broker.

---

# 45. Partition Leases

A claim MUST have a finite lease.

Default:

```yaml
batch:
  distributed:
    partition:
      lease-duration: 30s
      lease-heartbeat: 10s
```

While processing, the worker renews:

```text
lease_expires_at
```

If a pod dies:

```text
heartbeat stops
lease expires
partition becomes recoverable
```

---

# 46. Fencing Token

A lease alone is insufficient.

Example:

```text
worker A pauses
lease expires
worker B claims partition
worker A resumes
```

Without protection both may write progress.

Therefore every claim MUST increment:

```text
lease_version
```

Worker writes MUST use:

```sql
WHERE id = :partitionId
  AND worker_id = :workerId
  AND lease_version = :leaseVersion
  AND status = 'RUNNING'
```

If affected row count is zero:

```text
worker lost ownership
```

The worker MUST abort processing immediately.

---

# 47. Partition Recovery

A maintenance/reaper service MUST periodically find:

```text
status IN ('CLAIMED', 'RUNNING')
AND lease_expires_at < now()
```

and transition them to:

```text
RETRYABLE
```

The reaper MUST preserve:

```text
checkpoint_byte
checkpoint_record_no
committed counters
```

A new worker resumes from the last committed checkpoint.

---

# 48. Graceful Scale Down

On SIGTERM:

1. mark worker `DRAINING`,
2. stop claiming new partitions,
3. allow the current chunk to commit,
4. persist checkpoint,
5. if the partition is incomplete, release it as `RETRYABLE`,
6. mark worker `STOPPED`,
7. terminate.

The Kubernetes termination grace period MUST be greater than the maximum expected chunk completion time plus safety margin.

---

# 49. Abrupt Pod Failure

For:

```text
kill -9
node failure
OOMKill
container crash
```

recovery relies on:

```text
worker heartbeat timeout
+
partition lease expiry
+
fencing token
+
checkpoint
```

No operator action should be required for an ordinary single-worker crash.

---

# 50. Distributed Processing and Spring Batch

Spring Batch remains the batch execution framework.

Recommended top-level job:

```text
weeklyCsvCoordinatorJob
```

Recommended steps:

```text
1. validateFileStep
2. indexAndCreatePartitionsStep
3. distributedProcessingManagerStep
4. reconcileStep
5. finalizeStep
```

The distributed processing layer uses Spring Batch partition concepts, but PostgreSQL acts as the remoting/coordination fabric.

No external message channel is used.

---

# 51. Custom PostgreSQL Partition Handler

Implement a custom Spring Batch `PartitionHandler`:

```java
PostgresPartitionHandler
```

Responsibilities:

1. receive/create partition `StepExecution` metadata,
2. persist corresponding partition work rows,
3. expose partition work to remote worker pods through PostgreSQL,
4. poll `JobRepository` and/or `control.batch_partition`,
5. wait until all partitions reach terminal state,
6. fail the manager step if unrecoverable partitions fail,
7. support restart.

Do not implement a custom partition splitter when Spring Batch `Partitioner`/`StepExecutionSplitter` can be reused.

---

# 52. Worker Step Execution

Recommended worker step:

```text
processCsv.worker
```

Each worker pod MUST have the same worker step bean available.

The worker executes one complete logical partition:

```text
ByteRangeCsvItemReader
        |
        v
CsvRecordProcessor
        |
        v
PartitionResultWriter
```

The worker step MUST be step-scoped.

---

# 53. Step Execution Request

The implementation SHOULD reuse Spring Batch remote-partition primitives where practical:

```text
StepExecutionRequest
StepExecutionRequestHandler
StepLocator
JobRepository
```

A database work row SHOULD reference:

```text
jobExecutionId
stepExecutionId
stepName
partitionId
```

The worker can translate a claimed database row into a Spring Batch step-execution request.

If framework API constraints make safe same-execution lease recovery impractical, the implementation MAY use a thin custom worker execution adapter, but the decision MUST be documented in an ADR and covered by restart/fencing tests.

---

# 54. Byte-Range CSV Reader

Implement a custom step-scoped reader:

```java
ByteRangeCsvItemReader
```

Recommended base contract:

```java
class ByteRangeCsvItemReader
        extends AbstractItemStreamItemReader<CsvRecord>
```

Inputs:

```text
resource
startByte
endByteExclusive
checkpointByte
checkpointRecordNumber
CSV schema/configuration
```

The reader MUST:

1. open a `SeekableByteChannel`,
2. seek directly to `checkpointByte`,
3. parse only complete logical records,
4. stop at `endByteExclusive`,
5. never read another partition's logical record,
6. expose the next safe byte position for checkpointing.

---

# 55. Header Handling

The header is outside distributed data partitions.

The indexer MUST determine:

```text
dataStartOffset = first logical record boundary after header
```

Schema information MUST be loaded centrally and passed to all workers.

Workers MUST NOT reinterpret an arbitrary partition start as a header.

---

# 56. Reader Restart State

At chunk commit, save:

```text
nextRecordByteOffset
nextLogicalRecordNumber
```

On retry, seek directly to the checkpoint byte.

No re-scan from partition start is required.

---

# 57. Chunk Processing

Worker step MUST be chunk-oriented.

Starting configuration:

```yaml
batch:
  processing:
    chunk-size: 2000
```

Benchmark:

```text
500
1,000
2,000
5,000
10,000
```

Chunk size affects checkpoint frequency, restart loss window, transaction cost, memory, and throughput.

---

# 58. Processor Requirements

The processor MUST be:

- deterministic,
- stateless where possible,
- thread-safe,
- side-effect-free where possible.

It MUST NOT perform a PostgreSQL query per CSV record.

For 10M records:

```text
1 DB query / record
=
10,000,000 DB queries
```

This is prohibited unless explicitly justified.

---

# 59. Reference Data

If validation/classification requires lookup data:

Preferred order:

1. preload small reference tables,
2. batch-fetch lookup data,
3. use immutable in-memory maps,
4. use Caffeine/local cache if required,
5. only then consider bounded DB access.

---

# 60. Current Writer Behavior

Message queues remain out of scope.

The worker pipeline SHOULD produce a lightweight `ProcessingResult`.

Example:

```java
public record ProcessingResult(
    long recordNumber,
    String businessKey,
    ProcessingStatus status,
    String reasonCode
) {}
```

The writer MUST NOT insert one PostgreSQL row per successful CSV record merely to prove processing occurred.

Default writer responsibility:

- aggregate classification counts,
- update partition committed progress,
- persist checkpoint metadata,
- expose extension points for future downstream integration.

---

# 61. Partition Progress Transaction

After a successful chunk:

```text
Spring Batch chunk commit
+
partition checkpoint
+
partition committed counters
```

SHOULD use the same PostgreSQL transaction manager where technically possible.

The implementation MUST ensure a chunk is not considered committed in `control.batch_partition` before the corresponding worker-step transaction succeeds.

---

# 62. Restart Semantics

Suppose a partition contains 125,000 records and the worker commits 80,000 before crashing.

Expected:

```text
checkpoint_record = start + 80,000
checkpoint_byte   = safe byte boundary
```

After lease expiry:

```text
partition -> RETRYABLE
```

Another pod claims it and resumes from the checkpoint.

It MUST NOT start from file byte zero.

---

# 63. Execution Semantics

Without an external side effect, the system guarantees:

```text
no missing partition
no overlapping partition ranges
one valid lease owner at a time
restart from committed checkpoint
final count reconciliation
```

Under crash recovery, a currently executing uncommitted chunk MAY be executed again.

Therefore:

```text
at-least-once computation at the uncommitted chunk boundary
```

The specification MUST NOT claim distributed exactly-once processing.

---

# 64. Coordinator Job Flow

```text
Quartz
  |
  v
weeklyCsvCoordinatorJob
  |
  +--> validateFileStep
  |
  +--> indexAndCreatePartitionsStep
  |
  +--> distributedProcessingManagerStep
  |
  +--> reconcileStep
  |
  +--> finalizeStep
```

---

# 65. validateFileStep

Responsibilities:

- resolve input resource,
- verify existence,
- verify readability,
- verify stability window,
- validate file naming rules,
- create/reuse manifest,
- prevent duplicate completed generation,
- verify expected header/schema,
- move manifest to `VALIDATING`.

---

# 66. indexAndCreatePartitionsStep

Responsibilities:

1. count active worker slots,
2. calculate target partition count,
3. scan the file once,
4. calculate SHA-256,
5. count logical records,
6. validate CSV record boundaries,
7. generate safe partition boundaries,
8. verify file metadata unchanged,
9. persist partitions,
10. validate partition coverage,
11. change manifest to `READY`.

This step is intentionally single-coordinator.

---

# 67. distributedProcessingManagerStep

Responsibilities:

- change manifest to `PROCESSING`,
- make partitions claimable,
- monitor active workers,
- monitor partition progress,
- requeue expired partitions,
- detect terminal failures,
- wait for all partitions to become `COMPLETED`,
- fail when retry thresholds are exceeded.

The manager MUST NOT assign fixed partition-to-pod mappings.

---

# 68. Work Stealing

Workers repeatedly:

```text
claim
process
complete
claim next
```

The exact partition-to-pod mapping is not predetermined.

This automatically balances heterogeneous processing time.

---

# 69. New Pod Joins Mid-Job

Scenario:

```text
job starts with 5 pods
administrator scales Deployment to 15
```

Expected:

1. 10 new pods start,
2. each registers in `control.batch_worker`,
3. each begins heartbeats,
4. each becomes `IDLE`,
5. each starts claiming remaining partitions,
6. no repartition of already claimed work is required.

This is a mandatory acceptance test.

---

# 70. Pod Leaves Mid-Job

Scenario:

```text
15 pods -> scale down to 5
```

Expected graceful case:

- terminating workers stop claiming,
- checkpoint current work,
- release incomplete partitions,
- remaining pods continue.

Expected forced case:

- leases expire,
- remaining pods reclaim unfinished work.

The weekly job should continue unless no active workers remain beyond a configurable timeout.

---

# 71. Zero Worker Handling

Configure:

```yaml
batch:
  distributed:
    no-worker-timeout: 5m
```

If no active worker slot exists for longer than the timeout:

```text
FAIL distributed step
```

---

# 72. Partition Retry Limit

Configuration:

```yaml
batch:
  distributed:
    partition:
      max-attempts: 3
```

When attempts exceed the configured limit:

```text
partition -> FAILED
```

The manager step MUST fail.

---

# 73. Invalid CSV Records

Policy MUST be configurable.

Recommended options:

```text
FAIL_FAST
SKIP_UP_TO_LIMIT
```

All skipped records MUST be counted.

Successful records MUST NOT be logged individually.

---

# 74. Partition Completion

A partition can become `COMPLETED` only when:

```text
reader reached endByteExclusive
AND
processed count reconciles
AND
final chunk committed
AND
worker still owns current fencing token
```

Completion update MUST include the current `worker_id` and `lease_version`.

---

# 75. Per-Partition Reconciliation

For each partition:

```text
expectedRecordCount
=
acceptedCount
+ rejectedCount
+ invalidCount
+ explicitlyFilteredCount
```

depending on the business outcome model.

If the equation fails, the partition MUST NOT be completed.

---

# 76. Global Reconciliation

`reconcileStep` MUST verify:

```text
all partitions == COMPLETED
```

and:

```text
SUM(partition.expectedRecordCount)
=
manifest.expectedRecordCount
```

and:

```text
SUM(partition.committedProcessedCount)
=
manifest.expectedRecordCount
```

subject to the configured invalid/skip model.

It MUST also revalidate:

```text
partition ranges contiguous
no gaps
no overlaps
```

---

# 77. Run Summary

Create:

```text
control.batch_run_summary
```

Recommended fields:

```text
job_execution_id
manifest_id
business_date
active_workers_at_start
maximum_workers_observed
partition_count
record_count
accepted_count
rejected_count
invalid_count
retry_count
lease_expiration_count
started_at
finished_at
duration_ms
status
```

---

# 78. File Modification Detection During Processing

At minimum each worker MUST verify at partition start and before partition completion:

```text
file size == manifest.file_size_bytes
lastModified == manifest.last_modified_at
```

If the metadata changes:

```text
FAIL partition
FAIL weekly job
```

---

# 79. Stronger File Integrity Mode

Optional future config:

```yaml
batch:
  input:
    integrity:
      mode: METADATA
```

Future:

```text
PARTITION_HASH
```

Do not require each pod to hash the entire file independently.

---

# 80. PostgreSQL Connection Strategy

PostgreSQL is used for:

- Quartz metadata,
- Spring Batch metadata,
- worker heartbeat,
- partition claim,
- partition lease,
- checkpoint/counters,
- reconciliation.

Processing MUST NOT require a DB call per CSV record.

Pool sizing MUST consider all 20 pods.

---

# 81. Transaction Scope

Never hold a database transaction open while:

- reading an entire partition,
- waiting for another worker,
- sleeping for lease renewals.

Transactions MUST be short.

Partition claim, lease renewal, and chunk commit are bounded transactions.

---

# 82. Advisory Locks

PostgreSQL advisory locks MAY be used as defense-in-depth for short coordinator-only critical sections.

They MUST NOT be the primary long-running worker ownership mechanism.

Primary ownership is:

```text
row state
+
lease
+
fencing token
```

---

# 83. Thread Model

V1 default:

```text
one partition step per pod
```

Inside a partition:

```text
sequential reader
+
chunk-oriented processing
```

Do not immediately add multi-threaded processing inside each pod.

Scale horizontally first.

---

# 84. Java 25 Virtual Threads

Default for the core CSV path:

```text
platform threads
```

Parsing and condition evaluation are primarily CPU work.

Virtual threads MAY be tested for auxiliary blocking tasks.

They MUST NOT be used to create one virtual thread per record.

---

# 85. Task vs Thread Model

Example:

```text
80 partition tasks
10 worker pods
1 worker thread per pod
```

The system gradually executes 80 tasks on 10 execution resources.

It does NOT create:

```text
10M records -> 10M threads
```

---

# 86. Application Configuration

Recommended:

```yaml
batch:
  schedule:
    enabled: ${BATCH_SCHEDULE_ENABLED:true}
    cron: ${BATCH_CRON:0 0 6 ? * MON}
    zone: ${BATCH_TIME_ZONE:Asia/Ho_Chi_Minh}

  input:
    resource: ${BATCH_INPUT_RESOURCE:classpath:/batch/weekly.csv}
    encoding: ${BATCH_INPUT_ENCODING:UTF-8}
    stability-window: ${BATCH_INPUT_STABILITY_WINDOW:10m}

  processing:
    chunk-size: ${BATCH_CHUNK_SIZE:2000}
    invalid-record-policy: ${BATCH_INVALID_POLICY:FAIL_FAST}
    skip-limit: ${BATCH_SKIP_LIMIT:0}

  distributed:
    worker:
      concurrency: ${BATCH_WORKER_CONCURRENCY:1}
      heartbeat-interval: ${BATCH_WORKER_HEARTBEAT:5s}
      active-timeout: ${BATCH_WORKER_ACTIVE_TIMEOUT:20s}
      idle-poll-interval: ${BATCH_WORKER_IDLE_POLL_INTERVAL:1s}

    partition:
      partitions-per-active-worker: ${BATCH_PARTITIONS_PER_WORKER:8}
      max-pods: ${BATCH_MAX_PODS:20}
      scale-headroom-partitions-per-max-pod: ${BATCH_SCALE_HEADROOM:2}
      max-partitions: ${BATCH_MAX_PARTITIONS:320}
      min-partition-bytes: ${BATCH_MIN_PARTITION_BYTES:32MB}
      lease-duration: ${BATCH_PARTITION_LEASE:30s}
      lease-heartbeat: ${BATCH_PARTITION_LEASE_HEARTBEAT:10s}
      max-attempts: ${BATCH_PARTITION_MAX_ATTEMPTS:3}

    no-worker-timeout: ${BATCH_NO_WORKER_TIMEOUT:5m}
```

Typed `@ConfigurationProperties` MUST be used.

---

# 87. Production Example

Environment:

```text
BATCH_INPUT_RESOURCE=file:/data/weekly/weekly.csv
BATCH_CRON=0 0 6 ? * MON
BATCH_TIME_ZONE=Asia/Ho_Chi_Minh
BATCH_MAX_PODS=20
```

Kubernetes volume:

```yaml
volumeMounts:
  - name: weekly-input
    mountPath: /data/weekly
    readOnly: true
```

All pods MUST receive the same mount.

---

# 88. Read-Only Production File Access

cronjob-app SHOULD mount the production input as read-only.

This prevents accidental corruption and strengthens the immutable-file contract.

---

# 89. Legacy Upgrade Feature Flags

Recommended:

```yaml
batch:
  legacy:
    enabled: ${BATCH_LEGACY_ENABLED:false}

  distributed:
    enabled: ${BATCH_DISTRIBUTED_ENABLED:true}
```

The legacy and distributed schedulers MUST NOT process the same file generation simultaneously.

---

# 90. Package Structure

Recommended:

```text
src/main/java/com/company/cronjob/
|
+-- configuration/
|   +-- BatchConfiguration.java
|   +-- QuartzConfiguration.java
|   +-- DistributedBatchProperties.java
|   +-- WorkerConfiguration.java
|
+-- scheduler/
|   +-- WeeklyCsvQuartzJob.java
|   +-- WeeklyScheduleConfiguration.java
|
+-- batch/
|   +-- coordinator/
|   |   +-- WeeklyCsvCoordinatorJobConfiguration.java
|   |   +-- ValidateFileTasklet.java
|   |   +-- IndexAndCreatePartitionsTasklet.java
|   |   +-- DistributedProcessingManagerTasklet.java
|   |   +-- ReconcileTasklet.java
|   |   +-- FinalizeTasklet.java
|   |
|   +-- partition/
|   |   +-- CsvPartitionPlan.java
|   |   +-- CsvPartition.java
|   |   +-- CsvBoundaryIndexer.java
|   |   +-- CsvBoundaryIndexerImpl.java
|   |   +-- PostgresPartitionHandler.java
|   |   +-- PartitionClaimService.java
|   |   +-- PartitionLeaseService.java
|   |   +-- PartitionRecoveryService.java
|   |
|   +-- worker/
|       +-- DistributedWorkerLifecycle.java
|       +-- WorkerHeartbeatService.java
|       +-- WorkerPollingService.java
|       +-- ByteRangeCsvItemReader.java
|       +-- CsvRecordProcessor.java
|       +-- PartitionResultWriter.java
|       +-- PartitionCheckpointService.java
|
+-- input/
|   +-- BatchInputResourceResolver.java
|   +-- ClasspathResourceMaterializer.java
|   +-- FileFingerprintService.java
|
+-- persistence/
|   +-- BatchWorkerRepository.java
|   +-- BatchPartitionRepository.java
|   +-- BatchFileManifestRepository.java
|   +-- BatchRunSummaryRepository.java
|
+-- observability/
|   +-- BatchMetrics.java
|   +-- PartitionMetrics.java
|
+-- exception/
    +-- FileChangedException.java
    +-- PartitionLeaseLostException.java
    +-- PartitionIntegrityException.java
    +-- CsvBoundaryException.java
```

---

# 91. Native SQL for Claiming

The claim repository SHOULD use Spring JDBC/native SQL.

Do not hide `FOR UPDATE SKIP LOCKED` behind unnecessary ORM abstraction.

Recommended:

```text
JdbcTemplate
NamedParameterJdbcTemplate
JdbcClient
```

according to project conventions.

---

# 92. Fencing Updates

Methods such as:

```text
renewLease
commitCheckpoint
completePartition
releasePartition
```

MUST return affected-row count.

Expected:

```text
1 = ownership valid
0 = lease/fence lost
```

Zero MUST cause a `PartitionLeaseLostException`.

---

# 93. Worker Polling

Workers MUST NOT busy-spin.

When no work exists, polling MAY use exponential backoff.

PostgreSQL `LISTEN/NOTIFY` MAY be added later only as a wake-up optimization; database rows remain the durable source of truth.

---

# 94. Coordinator Failure

Scenario:

```text
coordinator pod crashes
workers still processing
```

Requirements:

1. Spring Batch metadata persists coordinator state.
2. Partition rows remain durable.
3. workers may continue while leases are valid,
4. recovery detects an orphaned coordinator execution,
5. job restart uses the same identifying parameters,
6. completed partitions are not recreated,
7. unfinished partitions remain/re-enter claimable state.

This scenario is mandatory in failure testing.

---

# 95. Manager Restart Behavior

On restart, if the manifest/index already exists and is valid:

- reuse partition definitions,
- do not re-index unnecessarily,
- do not reset completed partitions,
- re-enable retryable partitions,
- continue waiting for final completion.

---

# 96. File Generation Changes

If:

```text
same path
different SHA-256
```

then it is a new generation.

Create:

```text
new manifest
new Spring Batch JobInstance
new partition set
```

---

# 97. Sunday-to-Monday Contract

Sunday:

```text
upstream writes new generation
upstream closes file
upstream atomically publishes final filename
```

Monday:

```text
validate
index
process
reconcile
complete
```

The final file MUST NOT be mutated during Monday processing.

---

# 98. Worker Metrics

Expose:

```text
batch_workers_active
batch_workers_idle
batch_workers_busy
batch_workers_draining
batch_worker_heartbeat_age_seconds
```

---

# 99. Partition Metrics

Expose:

```text
batch_partitions_total
batch_partitions_ready
batch_partitions_running
batch_partitions_completed
batch_partitions_retryable
batch_partitions_failed
batch_partition_claim_total
batch_partition_reclaim_total
batch_partition_lease_expiration_total
batch_partition_duration_seconds
batch_partition_records_per_second
```

---

# 100. File/Job Metrics

Expose:

```text
batch_file_size_bytes
batch_file_expected_records
batch_index_duration_seconds
batch_job_duration_seconds
batch_records_processed_total
batch_records_per_second
batch_job_active_workers
batch_job_max_workers
batch_job_partition_skew_seconds
```

---

# 101. Progress Dashboard

Observability should support:

```text
Weekly CSV Job

Records:
6,820,000 / 10,042,310

Active pods:
10

Partitions:
80 total
51 completed
10 running
19 ready

Throughput:
24,500 records/s

Lease expirations:
1

Retries:
1
```

---

# 102. Logging

Structured logs SHOULD include:

```text
jobExecutionId
manifestId
fileSha256
workerId
podName
partitionId
leaseVersion
stepName
```

Do not log every successful record.

---

# 103. Error Logging

Malformed-record logs must be bounded and sampled.

All errors must be counted in metrics.

Sensitive CSV content MUST be redacted.

---

# 104. Tracing

Recommended:

```text
Quartz trigger
  -> coordinator job
     -> index step
     -> distributed manager
        -> sampled partition
           -> sampled chunk
```

Do not create one span per record by default.

---

# 105. Actuator / Health

Expose:

```text
/actuator/health
/actuator/metrics
/actuator/prometheus
```

Readiness requires PostgreSQL.

Absence of Sunday's file outside the processing window SHOULD NOT make application liveness fail.

---

# 106. Security

Production MUST:

- run as non-root,
- use read-only source mount,
- keep DB credentials in Secrets,
- reject path traversal,
- restrict accepted file locations/schemes,
- avoid logging sensitive data,
- use least-privilege runtime DB credentials.

---

# 107. PostgreSQL Permissions

Runtime app identity requires DML on:

```text
quartz
batch
control
```

It SHOULD NOT require production DDL privileges.

---

# 108. Indexing Performance

The indexer MUST:

- use buffered/channel I/O,
- avoid allocation per byte,
- calculate SHA-256 incrementally,
- store only O(partition count) boundaries,
- never store all 10M record offsets.

---

# 109. Memory

Memory MUST remain bounded approximately by:

```text
chunk size
x local worker slots
x record/result object size
+
parser buffers
+
reference data
+
framework overhead
```

It MUST NOT scale with total file size.

---

# 110. Scaling Expectations

The architecture supports 1–20 pods but MUST NOT promise linear speedup.

Shared bottlenecks may include:

- shared filesystem,
- CPU,
- PostgreSQL,
- reference-data access.

Scaling must be benchmarked.

---

# 111. Benchmark Matrix

Run:

```text
1 pod
2 pods
5 pods
10 pods
20 pods
```

using the same representative 10M-record file.

Record:

- total duration,
- index duration,
- distributed-processing duration,
- records/sec,
- CPU,
- memory,
- DB connections,
- DB CPU,
- storage throughput,
- partition skew,
- lease contention,
- claim latency.

---

# 112. Scaling Efficiency

Calculate:

```text
speedup(N) = duration(1) / duration(N)
efficiency(N) = speedup(N) / N
```

Identify the saturation point.

Do not scale to 20 if 10 produces no meaningful benefit.

---

# 113. Index Overhead

Track:

```text
index_time / total_job_time
```

If indexing dominates, a future optimization may perform indexing/checksum during Sunday's ingestion stage.

That is out of scope for V1.

---

# 114. Database Performance

Heartbeat and claim queries MUST have supporting indexes.

Coordinator worker-count refresh should happen every several seconds, not per record/chunk.

---

# 115. Partition Size Tuning

Partition processing time SHOULD generally be long enough that coordination overhead is small but short enough for work stealing and recovery.

Starting target:

```text
30 seconds to several minutes per partition
```

Benchmark before finalizing.

---

# 116. Chunk vs Partition

Example:

```text
10M records
80 partitions
~125K records per partition
2,000 records per chunk
```

Partition:

```text
distributed scheduling unit
```

Chunk:

```text
transaction/checkpoint unit
```

---

# 117. Worker Local Concurrency

V1:

```text
1 partition per pod
```

Only after benchmarking may:

```text
2+ partitions per pod
```

be enabled.

If concurrency increases, `worker_slots` MUST advertise the real capacity.

---

# 118. Backpressure

Backpressure exists because:

- worker slots are bounded,
- chunks are bounded,
- unclaimed partitions stay in PostgreSQL.

Do not copy the entire partition queue into memory.

---

# 119. Prohibited `parallelStream()`

The core batch path MUST NOT use `parallelStream()`.

---

# 120. Prohibited Unbounded Executors

No unbounded executor queue may contain CSV records or partitions.

---

# 121. PostgreSQL Isolation

`READ COMMITTED` with `FOR UPDATE SKIP LOCKED` is the recommended starting point for partition claiming.

Integration tests MUST verify behavior against the application's real PostgreSQL configuration.

---

# 122. Failure Test — Worker Crash

Test:

```text
10 pods
kill pod-5 at 50% of a partition
```

Expected:

```text
lease expires
partition -> RETRYABLE
another pod claims
resume from checkpoint
job completes
```

---

# 123. Failure Test — Coordinator Crash

Kill the coordinator after partition creation.

Expected:

- partition metadata survives,
- worker progress survives,
- coordinator recovery reuses completed work,
- job completes after restart.

---

# 124. Failure Test — Scale Up

Test:

```text
start with 1 pod
begin job
scale to 20 while significant work remains
```

Expected:

- new pods heartbeat,
- new pods claim remaining work,
- no duplicate valid ownership.

---

# 125. Failure Test — Scale Down

Test:

```text
20 -> 3 pods
```

Expected:

- graceful partitions are checkpointed/released,
- abrupt lost leases are reclaimed,
- 3 pods finish remaining work.

---

# 126. Failure Test — PostgreSQL Outage

Expected:

- no unsafe completion without durable checkpoint,
- active chunk rolls back when necessary,
- work resumes safely after DB recovery.

---

# 127. Failure Test — File Changed

Modify file metadata during processing.

Expected:

```text
partition failure
weekly job failure
alert
```

---

# 128. Failure Test — Duplicate Trigger

Attempt duplicate weekly launch.

Protection:

1. Quartz cluster,
2. `@DisallowConcurrentExecution`,
3. Spring Batch JobInstance,
4. manifest uniqueness.

Only one logical generation may process.

---

# 129. Failure Test — Multiline Boundary

Place quoted multiline records around planned partition cut points.

Expected:

- no record split,
- no record loss,
- final count matches index count.

---

# 130. Unit Tests

Must cover:

- quote-state machine,
- LF/CRLF,
- escaped quotes,
- multiline fields,
- UTF-8,
- header boundary,
- partition formula,
- file stability,
- worker timeout,
- fencing,
- state transitions,
- reconciliation.

---

# 131. PostgreSQL Integration Tests

Use Testcontainers PostgreSQL.

Must verify:

- Flyway,
- worker registration,
- heartbeat expiry,
- concurrent claims,
- `SKIP LOCKED`,
- fencing,
- lease reclaims,
- manifest uniqueness,
- duplicate job prevention.

Do not replace these with H2.

---

# 132. Multi-Instance Tests

Start multiple application instances against the same PostgreSQL.

CI:

```text
2–5 workers
```

Pre-production:

```text
up to 20 workers
```

---

# 133. Spring Batch Tests

Test:

- coordinator flow,
- worker step checkpoint,
- restart,
- failed partition,
- manager restart,
- reconciliation.

---

# 134. Performance Dataset

Generate:

```text
100K
1M
10M
20M
```

realistic records.

Include multiline/quoted records if supported by contract.

---

# 135. Performance Regression Gate

Changes to parser, reader, partitioner, chunk size, checkpoint code, worker coordination, or claim SQL SHOULD run performance regression tests.

---

# 136. Implementation Phase 0 — Legacy Baseline

Document:

- current cron mechanism,
- current parser,
- CSV schema,
- 10M runtime,
- current failure behavior,
- current transactions,
- business outputs.

Deliver:

```text
docs/legacy-baseline.md
```

---

# 137. Phase 1 — Runtime Upgrade

Upgrade:

```text
Java 25
Spring Boot 4.x
Spring Batch 6.x
Quartz
PostgreSQL
Flyway
Actuator
Testcontainers
```

Keep business behavior unchanged first.

---

# 138. Phase 2 — Input Abstraction

Implement:

```text
classpath:
file:
```

resource resolution and classpath materialization.

---

# 139. Phase 3 — Quartz JDBC Cluster

Implement:

- stable JobKey,
- JDBC JobStore,
- clustering,
- timezone,
- misfire,
- concurrent execution protection.

Test with two app instances.

---

# 140. Phase 4 — Worker Registry

Create control tables and implement registration/heartbeat.

Exit criterion:

```text
all active pods are accurately observable from PostgreSQL
```

---

# 141. Phase 5 — One-Partition Baseline

Run the new reader/processor/checkpoint path using one partition and one worker.

Prove correctness before distribution.

---

# 142. Phase 6 — CSV Boundary Indexer

Implement:

- SHA-256,
- logical record count,
- safe boundaries,
- coverage validation.

Do not proceed until quote/multiline tests pass.

---

# 143. Phase 7 — PostgreSQL Claiming

Implement:

- `SKIP LOCKED`,
- lease,
- fencing,
- retry,
- reaper.

Stress-test concurrent claims.

---

# 144. Phase 8 — Distributed Worker Loop

All pods:

```text
poll -> claim -> process -> checkpoint -> complete -> claim
```

Test with 2, 5, 10 instances.

---

# 145. Phase 9 — Dynamic Scaling

Test during a running job:

```text
1 -> 5
5 -> 20
20 -> 5
5 -> 1
```

No coordinator restart should be required.

---

# 146. Phase 10 — Failure Recovery

Inject:

- worker kill,
- coordinator kill,
- DB outage,
- SIGTERM,
- file mutation.

---

# 147. Phase 11 — Observability

Add:

- metrics,
- dashboards,
- alerts,
- structured logging,
- runbook.

---

# 148. Phase 12 — Production Cutover

Recommended:

1. deploy distributed code with schedule disabled,
2. validate migrations,
3. validate worker registry,
4. run manual test,
5. run full-scale test,
6. disable legacy schedule,
7. enable new Quartz schedule,
8. monitor first Monday run,
9. retain rollback flag.

---

# 149. Rollback

Rollback MUST preserve:

```text
Spring Batch metadata
partition metadata
file manifest
```

Never enable legacy processing for a generation while distributed processing is still active.

---

# 150. ADRs

Create:

```text
docs/adr/
```

Recommended:

```text
0001-quartz-jdbc-cluster-for-weekly-scheduling.md
0002-postgresql-worker-registry-instead-of-kubernetes-api.md
0003-postgresql-as-distributed-partition-fabric.md
0004-safe-csv-byte-boundary-indexing.md
0005-overpartition-and-work-stealing.md
0006-lease-and-fencing-for-partition-ownership.md
0007-classpath-only-for-development.md
0008-shared-readonly-production-file.md
0009-platform-threads-for-worker-processing.md
0010-at-least-once-uncommitted-chunk-semantics.md
```

---

# 151. Operational Runbook

Create:

```text
docs/runbook/weekly-csv-batch.md
```

Must cover:

- inspect active workers,
- inspect manifest,
- inspect partitions,
- identify expired leases,
- restart a failed job,
- scale 1–20 pods,
- safely scale down,
- verify Sunday file,
- investigate reconciliation failure,
- disable scheduler,
- rollback.

---

# 152. Alerting

Alert on:

```text
job did not start
job failed
no active workers
partition retries exceeded
abnormal lease expirations
file changed
reconciliation failure
duration exceeded SLO
throughput below baseline
PostgreSQL unavailable
Quartz misfire
```

---

# 153. Recommended SLO

Until business provides a formal SLA, use a configurable engineering target.

Example:

```text
10M representative records within 60 minutes
```

The final production SLO MUST be established after benchmarking.

---

# 154. Acceptance Criteria

1. One pod can process the full weekly CSV.
2. Twenty pods can participate in the same generation.
3. Active worker count is discovered through PostgreSQL heartbeats.
4. New pods can join a running job.
5. Scale-down does not lose unfinished work.
6. No partition has two valid fencing owners simultaneously.
7. An expired worker cannot commit after a newer lease exists.
8. Partition ranges have no gaps.
9. Partition ranges have no overlaps.
10. No boundary splits a logical CSV record.
11. Multiline CSV records are safe at boundaries.
12. Partition expected counts sum to the file count.
13. Final processed counts reconcile.
14. A killed worker is automatically recovered.
15. Recovery resumes from committed checkpoint.
16. File changes are detected.
17. A completed file generation cannot accidentally run twice.
18. Quartz nodes do not create duplicate weekly runs.
19. Memory remains bounded for 10M records.
20. No per-record PostgreSQL query exists in the normal path.
21. No one-thread-per-record model exists.
22. No unbounded in-memory partition queue exists.
23. Classpath mode works for development.
24. Production `file:/...` mode works.
25. 1, 2, 5, 10, and 20 pod benchmarks are recorded.
26. Worker, partition, throughput, retry, and lease metrics exist.
27. Coordinator restart preserves completed work.
28. PostgreSQL is the only distributed coordination dependency.
29. Legacy scheduling can be disabled safely.
30. Final reconciliation proves complete file coverage.

---

# 155. Definition of Done

The feature is complete when:

1. legacy app runs on Java 25 + Spring Boot 4,
2. clustered Quartz owns the weekly schedule,
3. all pods register and heartbeat through PostgreSQL,
4. active worker count is observable,
5. one CSV generation is indexed into safe byte ranges,
6. partition count adapts to worker count with scale headroom,
7. all pods dynamically claim work,
8. leases and fencing prevent stale ownership,
9. workers checkpoint at chunk boundaries,
10. dead workers recover automatically,
11. scale-up joins an in-progress job,
12. scale-down does not lose work,
13. final counts reconcile,
14. production file path is env-configurable,
15. classpath mode works for development,
16. integration/failure/load tests pass,
17. metrics and runbook exist,
18. legacy cron can be safely disabled.

---

# 156. Final Reference Flow

```text
Sunday
  |
  v
upstream publishes immutable weekly.csv
  |
  v
shared filesystem
  |
Monday 06:00
  |
  v
Quartz JDBC cluster
  |
one pod acquires trigger
  |
  v
weeklyCsvCoordinatorJob
  |
  v
validate stable file
  |
  v
query PostgreSQL worker registry
  |
active pods = W
  |
  v
calculate partition target
  |
  v
one CSV-aware indexing scan
  |
  +-- SHA-256
  +-- logical record count
  +-- safe byte boundaries
  |
  v
persist N READY partitions
  |
  v
all pods compete using
FOR UPDATE SKIP LOCKED
  |
  +----------------------+----------------------+----------------------+
  |                      |                      |                      |
  v                      v                      v                      v
pod-1                  pod-2                  pod-3                 pod-N
  |                      |                      |                      |
claim                   claim                  claim                  claim
  |                      |                      |                      |
lease + fence           lease + fence          lease + fence         lease + fence
  |                      |                      |                      |
seek range              seek range             seek range            seek range
  |                      |                      |                      |
process chunks          process chunks         process chunks        process chunks
  |                      |                      |                      |
checkpoint              checkpoint             checkpoint           checkpoint
  |                      |                      |                      |
complete                complete               complete             complete
  |                      |                      |                      |
claim next              claim next             claim next           claim next
  +----------------------+----------------------+----------------------+
                                 |
                                 v
                         all partitions complete
                                 |
                                 v
                             reconcile
                                 |
                                 v
                            COMPLETED
```

---

# 157. Final Architectural Principle

The system MUST optimize in this order:

```text
Data integrity
    >
Correct partition ownership
    >
Restartability
    >
Bounded resource usage
    >
Operational visibility
    >
Measured throughput
    >
Architectural cleverness
```

Horizontal scaling is implemented as:

```text
more pods
    ->
more registered worker slots
    ->
more concurrent partition claims
    ->
higher processing throughput
```

not as:

```text
more Quartz replicas
    ->
automatic record distribution
```

The required distributed model is:

```text
one weekly file
+
one coordinator
+
many safe partitions
+
many dynamic workers
+
PostgreSQL coordination
+
lease/fencing/checkpoint/reconciliation
```
