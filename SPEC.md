# SPEC.md — High-Throughput Weekly CSV Batch Processing Platform

## 1. Document Status

- **Status:** Implementation Specification
- **Primary audience:** AI coding agents, Principal Engineers, Solution Architects, Backend Engineers, SRE/DevOps Engineers
- **Language:** English
- **Target runtime:** Java 25
- **Application framework:** Spring Boot 4.x
- **Batch framework:** Spring Batch 6.x
- **Scheduler:** Quartz Scheduler 2.x through Spring Boot integration
- **Metadata database:** PostgreSQL
- **Deployment target:** Kubernetes
- **Input:** One CSV file containing 10,000,000+ records
- **Schedule:** Every Monday morning
- **Message queue:** Explicitly out of scope for this specification

This document is normative. Statements using **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are implementation requirements.

---

# 2. Problem Statement

Build a production-grade Spring Boot application that executes a large CSV-processing workload every Monday morning.

The source file may contain more than 10 million records. Each record must be:

1. Read from the CSV.
2. Parsed and structurally validated.
3. Evaluated against deterministic business conditions.
4. Classified into a processing result.
5. Counted and persisted as batch execution metadata so that the job can be monitored, restarted, and audited.

The original downstream message-queue publishing requirement is intentionally excluded from this version. The design MUST preserve an extension point where a future downstream writer can be plugged in without changing the reader, processor, scheduling, restart, or partitioning architecture.

The system must prioritize:

- correctness,
- restartability,
- bounded resource consumption,
- predictable throughput,
- horizontal scalability,
- operational visibility,
- safe concurrency,
- idempotent job launching,
- resilience to application and infrastructure failures.

---

# 3. Goals

The implementation MUST provide:

1. A Quartz-based weekly scheduler.
2. Quartz persistence and clustering using PostgreSQL.
3. A Spring Batch job that can process 10M+ CSV records.
4. Persistent Spring Batch metadata using PostgreSQL.
5. Deterministic job identity using file identity and business date.
6. File validation before expensive processing begins.
7. Chunk-oriented processing.
8. Bounded concurrent execution.
9. Safe restart from checkpoints after failure.
10. No unbounded task queues.
11. No one-thread-per-record design.
12. Clear support for both platform threads and Java 25 virtual threads, with platform threads as the default for the core CPU-oriented batch path.
13. Metrics, structured logs, and tracing at job/step/chunk level.
14. Kubernetes-ready runtime behavior.
15. A performance-testable implementation.
16. A documented scaling path from one JVM to local partitioning and later remote partitioning.
17. A downstream-output abstraction that does not require a message broker in this version.

---

# 4. Non-Goals

The following are out of scope:

- Kafka, RabbitMQ, JMS, Pulsar, or any other message broker.
- End-to-end message delivery guarantees.
- Consumer-side idempotency.
- Outbox/CDC architecture.
- XA/distributed transactions.
- A web UI.
- Manual record editing.
- Arbitrary user-created schedules.
- Real-time stream processing.
- Processing the full file entirely in memory.
- Creating one Java thread or virtual thread per CSV record.

---

# 5. Core Architectural Principle: Task vs Thread

The implementation MUST preserve the distinction between a **task** and a **thread**.

## 5.1 Task

A task is a unit of work.

Examples:

- launch a batch job,
- process one chunk,
- execute one partition,
- validate a file,
- reconcile job statistics.

Conceptually:

```text
Task = WHAT should be executed
```

Java representations may include:

```java
Runnable
Callable<T>
```

Spring abstractions include:

```java
TaskExecutor
TaskScheduler
```

## 5.2 Thread

A thread is an execution resource that runs tasks.

Conceptually:

```text
Thread = WHERE / ON WHAT execution resource a task runs
```

A system may have:

```text
5,000 chunk tasks
8 worker threads
```

The eight worker threads execute the 5,000 tasks over time.

The implementation MUST NOT equate:

```text
number of records == number of threads
```

## 5.3 TaskExecutor vs TaskScheduler

Use the following separation:

```text
TaskScheduler / Quartz
    -> determines WHEN work starts

TaskExecutor
    -> determines HOW tasks are executed concurrently

Thread
    -> execution resource used by the executor
```

Quartz MUST NOT perform the 10M-record processing itself.

---

# 6. High-Level Architecture

```text
                         +---------------------------+
                         |        PostgreSQL         |
                         |                           |
                         | quartz.QRTZ_*             |
                         | batch.BATCH_*             |
                         | control.batch_file_*      |
                         +-------------+-------------+
                                       ^
                                       |
                  +--------------------+--------------------+
                  |                                         |
        +---------+----------+                    +---------+----------+
        | Quartz Instance A  |                    | Quartz Instance B  |
        | Spring Boot Pod A   |                    | Spring Boot Pod B  |
        +---------+----------+                    +---------+----------+
                  |                                         |
                  +--------------------+--------------------+
                                       |
                              Monday scheduled trigger
                                       |
                                       v
                            +----------------------+
                            | Quartz Launch Job    |
                            +----------+-----------+
                                       |
                                       | starts
                                       v
                            +----------------------+
                            | Spring Batch Job     |
                            +----------+-----------+
                                       |
                +----------------------+----------------------+
                |                                             |
                v                                             v
        validateInputStep                            processCsvStep
                                                       |
                                              sequential CSV reader
                                                       |
                                                bounded chunk tasks
                                                       |
                                             concurrent processors
                                                       |
                                             lightweight result writer
                                                       |
                                                       v
                                              reconcile/finalize
```

---

# 7. Recommended Version-1 Processing Strategy

The first production implementation MUST use:

> **Single sequential CSV reader + bounded concurrent chunk processing**

This is the default because it is safe for general CSV syntax, including quoted fields and multiline values, and avoids repeated scanning of the same large file.

The implementation MUST benchmark this design before introducing file partitioning.

## 7.1 Why this is the default

For a single CSV file, naive line-range partitioning can be unsafe.

Example:

```csv
1,"hello
world",100
```

A physical newline is not necessarily a logical CSV record boundary.

Naively splitting by newline or arbitrary byte position can corrupt records.

Also, starting multiple readers and asking each reader to skip N records can cause duplicated I/O:

```text
reader-1 scans 0 -> 2.5M
reader-2 scans 0 -> 5M
reader-3 scans 0 -> 7.5M
reader-4 scans 0 -> 10M
```

This is prohibited as a default optimization.

---

# 8. Scaling Strategy

Scaling MUST happen incrementally and based on measured bottlenecks.

## Level 1 — Single JVM, Sequential Read, Concurrent Chunk Processing

Default implementation:

```text
CSV file
   |
   v
single sequential reader
   |
   v
chunks
   |
   +----> worker thread
   +----> worker thread
   +----> worker thread
   +----> worker thread
```

Use a bounded `TaskExecutor`.

This level MUST be implemented first.

## Level 2 — Safe Local Partitioning

Local partitioning MAY be introduced only when:

1. performance tests prove the single-reader path is the bottleneck, and
2. the CSV contract allows safe partition boundaries.

Permitted strategies:

### Strategy A — Precomputed safe byte offsets

Perform one indexing pass that identifies logical record boundaries and records byte offsets.

Then create partitions:

```text
partition-0: byte 0 -> 1,050,000,000
partition-1: byte 1,050,000,001 -> 2,100,000,000
...
```

All ranges MUST begin and end on logical CSV record boundaries.

### Strategy B — One physical line equals one logical record

Allowed only when the input contract explicitly guarantees:

- no quoted multiline fields,
- stable encoding,
- stable newline convention.

The guarantee MUST be documented and tested.

## Level 3 — Upstream Sharding

The preferred large-scale architecture is for the upstream producer to provide:

```text
input/
  part-000.csv
  part-001.csv
  part-002.csv
  ...
  part-031.csv
```

Each shard becomes an independent Spring Batch partition.

Use `MultiResourcePartitioner` or an equivalent partitioner.

## Level 4 — Remote Partitioning

If one JVM cannot meet the SLA after profiling and safe local optimization, remote partitioning MAY distribute complete worker steps across Kubernetes pods.

Remote partitioning is preferred over remote chunking for this workload because every worker can own:

```text
ItemReader
ItemProcessor
ItemWriter
```

Remote chunking is NOT part of the initial implementation.

---

# 9. Functional Requirements

## FR-001 — Weekly Schedule

The job MUST run every Monday morning.

Default configuration:

```yaml
batch:
  schedule:
    cron: "0 0 6 ? * MON"
    zone: "Asia/Ho_Chi_Minh"
```

The cron expression and timezone MUST be configurable.

The application MUST NOT depend on the operating-system/container default timezone.

---

## FR-002 — Quartz Responsibility

Quartz MUST only:

1. determine when execution begins,
2. resolve the current input-file identity,
3. construct deterministic Spring Batch job parameters,
4. launch the Spring Batch job,
5. return control.

Quartz MUST NOT:

- iterate over CSV rows,
- execute business validation per row,
- own chunk logic,
- own retry logic for individual records.

---

## FR-003 — Persistent Quartz Job Store

Production MUST use Quartz JDBC JobStore.

Example logical configuration:

```yaml
spring:
  quartz:
    job-store-type: jdbc
    properties:
      org.quartz.scheduler.instanceId: AUTO
      org.quartz.jobStore.isClustered: true
```

Exact property names MUST be verified against the Spring Boot 4 and Quartz versions resolved by the project dependency management.

The implementation MUST use PostgreSQL-backed Quartz tables.

---

## FR-004 — Quartz Clustering

At least two application replicas MUST be supported.

All Quartz instances MUST share the same Quartz database schema.

Only one scheduler instance may successfully fire a given trigger occurrence.

The Quartz job class MUST prohibit concurrent execution of the same logical scheduled job.

Use:

```java
@DisallowConcurrentExecution
```

or the equivalent supported mechanism.

---

## FR-005 — Misfire Policy

The default business policy is:

> If the Monday execution was missed because all scheduler instances were unavailable, execute the job once when the scheduler becomes available.

The trigger MUST use an explicit misfire policy.

This behavior MUST be configurable.

Supported policies:

```text
FIRE_ONCE_NOW
DO_NOTHING
```

Default:

```text
FIRE_ONCE_NOW
```

---

# 10. Input File Contract

The file source MUST be represented behind an abstraction:

```java
public interface InputFileResolver {
    ResolvedInputFile resolve(LocalDate businessDate);
}
```

The implementation MUST NOT tightly couple the batch core to one storage provider.

Supported implementations MAY include:

- local filesystem for development,
- persistent volume,
- object storage,
- mounted enterprise file share.

`ResolvedInputFile` MUST contain at least:

```java
record ResolvedInputFile(
    URI uri,
    String fileName,
    long sizeBytes,
    Instant lastModified,
    String checksum
) {}
```

---

# 11. File Identity and Idempotency

A file execution MUST have deterministic identity.

At minimum:

```text
businessDate
fileChecksum
fileName or canonical URI
```

Recommended Spring Batch job parameters:

```text
businessDate = 2026-09-07
fileChecksum = SHA-256(...)
fileUri      = ...
```

The same logical file MUST produce the same identifying job parameters.

A completed job instance MUST NOT be silently processed again.

A currently running job instance MUST NOT be launched again.

A failed job instance MUST be restartable.

---

# 12. File Manifest

Create an application-owned control table:

```sql
control.batch_file_manifest
```

Recommended fields:

```text
id
business_date
file_name
file_uri
file_size_bytes
checksum_sha256
status
discovered_at
processing_started_at
processing_finished_at
record_count
created_at
updated_at
```

Recommended status enum:

```text
DISCOVERED
VALIDATING
READY
PROCESSING
COMPLETED
FAILED
QUARANTINED
```

Add a uniqueness constraint covering the chosen deterministic identity, for example:

```text
UNIQUE(business_date, checksum_sha256)
```

This table complements Spring Batch metadata; it does not replace `JobRepository`.

---

# 13. Input Validation Step

The first Spring Batch step MUST validate the input file before expensive processing.

Suggested job:

```text
weeklyCsvProcessingJob
  |
  +-- validateInputStep
  |
  +-- loadReferenceDataStep        optional
  |
  +-- processCsvStep
  |
  +-- reconcileStep
  |
  +-- finalizeStep
```

`validateInputStep` MUST verify:

- file exists,
- file is readable,
- file size is greater than zero,
- file is not still being modified,
- checksum is available,
- expected file naming convention,
- expected CSV header,
- expected delimiter,
- expected encoding,
- required columns are present,
- no duplicate completed manifest exists,
- optional sanity limits such as minimum/maximum size.

A validation failure MUST stop the job before the 10M-record processing phase begins.

---

# 14. Immutable Input Rule

The input MUST be immutable while processing.

Permitted strategies:

1. object-storage immutable object/version,
2. atomic rename from `incoming` to `processing`,
3. copy to a content-addressed location using checksum,
4. upstream completion marker.

Example:

```text
incoming/customers.csv
        |
        v
processing/customers-<sha256>.csv
```

The processing job MUST operate only on the immutable version.

---

# 15. Spring Batch Repository

Production MUST use a persistent JDBC `JobRepository`.

Use PostgreSQL.

The following conceptual entities MUST be persisted:

```text
JobInstance
JobExecution
StepExecution
ExecutionContext
```

The batch repository MUST NOT use an in-memory or resourceless implementation in production because restartability is a core requirement.

---

# 16. Database Schema Separation

Use separate PostgreSQL schemas:

```text
quartz
batch
control
```

Responsibilities:

```text
quartz.QRTZ_*
    scheduler state

batch.BATCH_*
    Spring Batch execution state

control.batch_file_manifest
    application-level file lifecycle and audit
```

Optionally:

```text
control.batch_run_summary
```

may store business-facing summary data.

---

# 17. Database Migrations

Production MUST NOT depend on framework auto-initialization of Quartz or Spring Batch schemas.

Use an explicit migration mechanism such as Flyway.

Recommended migration layout:

```text
db/migration/

V001__create_quartz_schema.sql
V002__create_quartz_tables.sql
V003__create_batch_schema.sql
V004__create_batch_tables.sql
V005__create_control_schema.sql
V006__create_batch_file_manifest.sql
V007__create_batch_run_summary.sql
```

The SQL for framework-owned tables MUST be based on the official PostgreSQL schema scripts corresponding to the resolved Quartz/Spring Batch versions.

Do not invent framework metadata schemas manually.

---

# 18. Batch Job Design

Recommended job:

```text
weeklyCsvProcessingJob
```

Recommended flow:

```text
START
  |
  v
validateInputStep
  |
  v
loadReferenceDataStep
  |
  v
processCsvStep
  |
  v
reconcileStep
  |
  v
finalizeStep
  |
  v
END
```

---

# 19. validateInputStep

Type:

```text
Tasklet
```

Responsibilities:

- acquire/resolve file,
- validate immutable state,
- verify manifest,
- validate header/schema,
- persist manifest state,
- expose validated file metadata through job execution context.

It MUST NOT scan all 10M records unless checksum generation is unavoidable.

If checksum generation requires a full scan, the implementation SHOULD calculate checksum as part of file ingestion whenever possible so the processing job does not pay an extra full-file I/O pass.

---

# 20. loadReferenceDataStep

This step is optional but strongly recommended if condition checking requires reference data.

It SHOULD:

- load stable lookup/reference information once,
- build an immutable in-memory representation,
- avoid per-record DB/REST calls,
- publish only small metadata or a cache key into execution context.

Do NOT serialize very large reference datasets into Spring Batch `ExecutionContext`.

Recommended options:

```text
immutable Map
Caffeine cache
application-local read-only index
```

The exact strategy depends on reference-data size.

---

# 21. processCsvStep

The core step MUST be chunk-oriented.

Concept:

```text
read N records
process N records
write N processing results
commit checkpoint

repeat
```

The chunk size MUST be configurable.

Starting benchmark values:

```text
500
1,000
2,000
5,000
10,000
```

No single value is considered universally optimal.

The final default MUST be chosen from measured results.

---

# 22. Reader

Use a streaming reader.

The reader MUST NOT load the entire CSV file into memory.

Preferred V1 behavior:

```text
single sequential stream
```

The parser MUST correctly support the agreed CSV contract.

Possible implementation approaches:

- Spring Batch flat-file components where sufficient,
- a robust CSV parser integrated with a custom `ItemReader` when complex quoting/multiline behavior is required.

The chosen parser MUST be benchmarked with representative production data.

The reader MUST persist restart information such as logical record position through Spring Batch checkpoint semantics.

---

# 23. Domain Input Model

Create a compact immutable record.

Example:

```java
public record CustomerCsvRecord(
    long logicalRecordNumber,
    String customerId,
    String fieldA,
    String fieldB,
    BigDecimal amount,
    LocalDate eventDate
) {}
```

Do not keep the raw CSV line unless required for diagnostics.

Avoid unnecessary object graphs.

Use Java records where appropriate.

---

# 24. Processor

The processor MUST be:

- deterministic,
- stateless wherever possible,
- thread-safe,
- side-effect-free wherever possible,
- independent of scheduling concerns.

Example abstraction:

```java
public interface RecordClassifier<I, O> {
    O classify(I input);
}
```

Example output:

```java
public record ProcessingResult(
    long logicalRecordNumber,
    String businessKey,
    ProcessingStatus status,
    String reasonCode
) {}
```

Example statuses:

```text
ACCEPTED
REJECTED_RULE_A
REJECTED_RULE_B
INVALID
SKIPPED
```

Business rejection MUST NOT automatically be treated as infrastructure failure.

---

# 25. Avoid Per-Record Remote Calls

The implementation MUST NOT perform one database or network request per record by default.

For 10M records:

```text
1 lookup / record = 10,000,000 lookups
2 lookups / record = 20,000,000 lookups
```

This can dominate runtime.

Required strategy:

1. preload reference data when feasible,
2. batch-load lookup information,
3. use bounded caches,
4. perform remote access only when unavoidable,
5. benchmark any unavoidable remote access separately.

---

# 26. Writer Without Message Queue

This version MUST implement a lightweight downstream-neutral writer.

Recommended interface:

```java
public interface ProcessingResultSink {
    void write(Chunk<? extends ProcessingResult> results);
}
```

Default implementation:

```text
ClassificationMetricsWriter
```

Responsibilities:

- count results by `ProcessingStatus`,
- persist restart-safe summary state at checkpoint boundaries,
- optionally persist compact run summaries,
- avoid storing all 10M results individually,
- avoid per-record logs.

No message broker may be required.

A future implementation MUST be able to replace or decorate this sink without changing the reader or processor.

---

# 27. Restart-Safe Counters

Classification totals MUST survive a failed execution and restart.

Do not rely only on process-local `LongAdder` values.

Possible implementation:

1. maintain in-memory counters per chunk,
2. update step execution context at successful checkpoint boundaries,
3. reconcile totals from step execution metadata,
4. optionally persist summary rows in `control.batch_run_summary`.

Example summary:

```text
run_id
business_date
file_checksum
read_count
accepted_count
rejected_count
invalid_count
skipped_count
started_at
finished_at
duration_ms
status
```

---

# 28. Concurrency Model

The system MUST use bounded concurrency.

Default core batch workers SHOULD use platform threads.

Example conceptual configuration:

```text
available processors = P

core workers ~= P
max workers  ~= P to 2P
queue size   = small bounded multiple of P
```

These are starting points only.

The implementation MUST expose tuning properties.

Example:

```yaml
batch:
  processing:
    chunk-size: 2000
    executor:
      core-pool-size: 8
      max-pool-size: 8
      queue-capacity: 16
```

Final values MUST be determined by benchmark results.

---

# 29. Dedicated Executor

Create a dedicated batch executor.

Do NOT reuse one global executor for:

- web requests,
- Quartz,
- batch chunks,
- unrelated `@Async` work.

Example bean:

```java
@Bean("batchProcessingExecutor")
ThreadPoolTaskExecutor batchProcessingExecutor(BatchProperties properties) {
    var executor = new ThreadPoolTaskExecutor();

    executor.setCorePoolSize(properties.executor().corePoolSize());
    executor.setMaxPoolSize(properties.executor().maxPoolSize());
    executor.setQueueCapacity(properties.executor().queueCapacity());
    executor.setThreadNamePrefix("batch-worker-");

    executor.setRejectedExecutionHandler(
        new ThreadPoolExecutor.CallerRunsPolicy()
    );

    executor.initialize();
    return executor;
}
```

The exact API MUST be adapted to the Spring Framework version used by Spring Boot 4.

---

# 30. Backpressure

No internal work queue may be unbounded.

Backpressure MUST exist between:

```text
reader
  ->
chunk scheduling
  ->
worker execution
  ->
writer
```

If workers cannot keep up, the reader path MUST eventually slow down rather than accumulating millions of tasks in memory.

Preferred properties:

- bounded executor queue,
- bounded number of in-flight chunks,
- caller-runs or equivalent throttling strategy,
- no unlimited `CompletableFuture` collection.

---

# 31. Java 25 Virtual Threads

Virtual threads MAY be used, but MUST NOT be enabled blindly.

Default policy:

```text
core CSV parsing + CPU-bound classification
    -> platform threads

blocking I/O helper workflows
    -> virtual threads MAY be benchmarked
```

Reasons:

- virtual threads do not create additional CPU cores,
- CPU-heavy work is limited by available processors,
- excessive concurrency may increase allocation, cache contention, and downstream pressure.

If virtual threads are enabled globally through Spring Boot, the implementation MUST verify that batch execution behavior remains bounded.

The team MUST benchmark:

```text
platform-thread batch executor
vs
virtual-thread executor
```

before changing the production default.

---

# 32. Thread Safety

Any component used concurrently MUST be explicitly reviewed for thread safety.

Particularly review:

- `ItemReader`,
- `ItemProcessor`,
- `ItemWriter`,
- CSV parser,
- date/number formatters,
- caches,
- mutable rule engines.

Prefer:

- immutable configuration,
- immutable reference data,
- stateless processors,
- step-scoped state when needed.

Do not share a non-thread-safe reader across worker threads.

---

# 33. Recommended Concurrent Chunking Pattern

The V1 implementation should preserve a single reader while processing chunks concurrently.

Conceptually:

```text
                    +----------------+
CSV -> reader ----> | chunk producer |
                    +-------+--------+
                            |
                   bounded in-flight chunks
                            |
             +--------------+--------------+
             |              |              |
             v              v              v
          worker-1       worker-2       worker-N
             |              |              |
             +--------------+--------------+
                            |
                            v
                           sink
```

The implementation MUST verify Spring Batch 6 transaction/checkpoint semantics for the selected concurrent-chunk API.

If the selected local-chunking mechanism weakens required retry/skip guarantees, use a safer single-threaded chunk step first and scale through partitioning instead.

Correctness takes priority over concurrency.

---

# 34. Error Classification

Errors MUST be separated into categories.

## 34.1 Business Rejection

Examples:

- condition not satisfied,
- customer not eligible,
- state incompatible with business rule.

Behavior:

```text
classify result
continue
```

Do not fail the job.

## 34.2 Invalid Input Record

Examples:

- malformed date,
- missing required value,
- invalid numeric field,
- structurally invalid CSV record.

Behavior is configurable:

```text
skip with count
or
fail after threshold
```

The implementation MUST support a configurable skip limit.

## 34.3 Infrastructure Failure

Examples:

- database unavailable,
- storage unavailable,
- unrecoverable filesystem error.

Behavior:

```text
retry bounded number of times
then fail step
```

The job MUST be restartable.

## 34.4 Programming / Invariant Failure

Examples:

- `NullPointerException`,
- impossible state,
- corrupt internal configuration.

Behavior:

```text
fail fast
```

Do not silently skip unknown programming failures.

---

# 35. Retry Policy

Retry MUST be:

- bounded,
- exception-specific,
- observable.

Do not retry deterministic parsing errors.

Use exponential backoff where appropriate for transient infrastructure failures.

Example conceptual policy:

```text
max attempts: 3
initial backoff: 500 ms
multiplier: 2
maximum backoff: 5 s
```

Values are configurable and must be justified through operational requirements.

---

# 36. Skip Policy

Skip policy MUST be explicit.

Example:

```yaml
batch:
  processing:
    skip-limit: 10000
```

If invalid records exceed the configured threshold, the step MUST fail.

The threshold prevents silently accepting severely corrupted input files.

---

# 37. Checkpoint and Commit Semantics

Chunk boundaries are restart checkpoints.

For example:

```text
chunk size = 2,000

records 1..2,000     committed
records 2,001..4,000 committed
records 4,001..6,000 failed
```

After restart, successfully committed chunks MUST NOT require full-file reprocessing.

The implementation MUST verify actual restart behavior with failure-injection tests.

---

# 38. Job Restart Semantics

Scenario:

```text
10,000,000 records
job crashes at 6,240,000
```

Expected behavior:

1. Job status becomes `FAILED`.
2. Spring Batch metadata preserves execution progress.
3. File manifest becomes `FAILED`.
4. Restart uses the same identifying parameters.
5. Previously committed work is not reprocessed beyond the expected checkpoint semantics.
6. Processing continues from the last valid checkpoint.
7. Final reconciliation produces one consistent run summary.

---

# 39. Duplicate Launch Protection

Use at least two protection layers:

## Layer 1 — Quartz

Prevent concurrent execution of the scheduled Quartz job.

## Layer 2 — Spring Batch

Use deterministic identifying job parameters so a running/completed JobInstance cannot be silently recreated.

## Layer 3 — File Manifest

Use a database uniqueness constraint on file identity.

These are complementary controls.

---

# 40. Reconciliation Step

After processing, execute a reconciliation step.

It MUST validate:

```text
read_count
=
accepted_count
+ rejected_count
+ invalid_count
+ filtered_count
+ other explicitly modeled outcomes
```

The exact equation depends on implementation semantics.

Reconciliation MUST fail the job if counters cannot be explained.

The result MUST be written to `control.batch_run_summary` or equivalent.

---

# 41. Finalize Step

On successful reconciliation:

- update manifest to `COMPLETED`,
- record completion timestamp,
- record duration,
- record final counts,
- optionally archive or mark input as processed.

On failure:

- manifest MUST remain restartable,
- original immutable input MUST remain available,
- no destructive cleanup may prevent restart.

---

# 42. Package / Module Structure

Recommended structure:

```text
src/main/java/com/example/batch/
|
+-- application/
|   +-- BatchApplication.java
|
+-- configuration/
|   +-- BatchConfiguration.java
|   +-- QuartzConfiguration.java
|   +-- ExecutorConfiguration.java
|   +-- DataSourceConfiguration.java
|   +-- ObservabilityConfiguration.java
|
+-- scheduler/
|   +-- WeeklyCsvQuartzJob.java
|   +-- WeeklyCsvScheduleFactory.java
|
+-- job/
|   +-- WeeklyCsvJobConfiguration.java
|   |
|   +-- validation/
|   |   +-- InputValidationTasklet.java
|   |
|   +-- reference/
|   |   +-- ReferenceDataLoader.java
|   |
|   +-- processing/
|   |   +-- CsvRecordReader.java
|   |   +-- CsvRecordProcessor.java
|   |   +-- ClassificationMetricsWriter.java
|   |   +-- ProcessingResultSink.java
|   |
|   +-- reconciliation/
|   |   +-- ReconciliationTasklet.java
|   |
|   +-- finalize/
|       +-- FinalizeBatchTasklet.java
|
+-- domain/
|   +-- CustomerCsvRecord.java
|   +-- ProcessingResult.java
|   +-- ProcessingStatus.java
|   +-- BatchFileManifest.java
|
+-- file/
|   +-- InputFileResolver.java
|   +-- LocalInputFileResolver.java
|   +-- FileChecksumService.java
|   +-- CsvSchemaValidator.java
|
+-- repository/
|   +-- BatchFileManifestRepository.java
|   +-- BatchRunSummaryRepository.java
|
+-- service/
|   +-- BatchLaunchService.java
|   +-- FileManifestService.java
|
+-- observability/
|   +-- BatchMetrics.java
|   +-- BatchLoggingListener.java
|   +-- BatchTracingListener.java
|
+-- exception/
    +-- InvalidInputFileException.java
    +-- BatchInvariantException.java
```

The project MAY be split into Gradle/Maven modules later, but the initial implementation SHOULD avoid premature multi-module complexity.

---

# 43. Configuration Properties

Create typed `@ConfigurationProperties`.

Example:

```yaml
batch:
  schedule:
    enabled: true
    cron: "0 0 6 ? * MON"
    zone: "Asia/Ho_Chi_Minh"

  input:
    location: "/data/incoming"
    expected-header:
      - customer_id
      - field_a
      - field_b
      - amount
      - event_date

  processing:
    chunk-size: 2000
    skip-limit: 10000

    executor:
      core-pool-size: 8
      max-pool-size: 8
      queue-capacity: 16

    partitioning:
      mode: SINGLE_READER
      grid-size: 8

  retry:
    max-attempts: 3
    initial-backoff: 500ms
    multiplier: 2.0
    max-backoff: 5s
```

Do not duplicate Spring Boot native configuration properties unnecessarily.

---

# 44. Dependency Management

Use the Spring Boot dependency-management/BOM mechanism.

Do not independently pin Spring Framework, Spring Batch, or Quartz transitive versions unless a documented compatibility reason exists.

Expected high-level dependencies:

```text
spring-boot-starter
spring-boot-starter-batch
spring-boot-starter-quartz
spring-boot-starter-jdbc
spring-boot-starter-actuator
spring-boot-starter-validation

PostgreSQL JDBC driver
Flyway
Micrometer registry
OpenTelemetry integration as selected
CSV parser if required
Testcontainers
```

Use either Maven or Gradle consistently.

---

# 45. Observability Requirements

The system MUST expose metrics for:

## Job metrics

```text
batch_job_started_total
batch_job_completed_total
batch_job_failed_total
batch_job_duration_seconds
```

## Record metrics

```text
batch_records_read_total
batch_records_processed_total
batch_records_accepted_total
batch_records_rejected_total
batch_records_invalid_total
batch_records_skipped_total
```

## Performance metrics

```text
batch_records_per_second
batch_chunk_duration_seconds
batch_reader_latency_seconds
batch_processor_latency_seconds
batch_writer_latency_seconds
batch_executor_active_threads
batch_executor_queue_depth
```

## Scheduler metrics

```text
quartz_trigger_fired_total
quartz_trigger_misfire_total
batch_duplicate_launch_prevented_total
```

Use Micrometer-compatible instrumentation.

---

# 46. Logging Requirements

Use structured logging.

Every important log event SHOULD contain:

```text
jobName
jobExecutionId
businessDate
fileChecksum
stepName
partitionId if applicable
```

Do not log each successful record.

Prohibited pattern:

```java
log.info("Processing record {}", record);
```

for millions of records.

Allowed:

- start/end events,
- periodic progress,
- summarized errors,
- sampled invalid records,
- job/step/partition state transitions.

Sensitive business fields MUST be redacted.

---

# 47. Progress Logging

Periodic progress MAY be emitted every configurable number of records or time interval.

Example:

```text
job=weeklyCsvProcessingJob
recordsRead=6,800,000
recordsProcessed=6,798,240
rate=18,400/s
elapsed=PT6M12S
```

Avoid excessive logging.

---

# 48. Tracing Requirements

Tracing granularity SHOULD be:

```text
Quartz trigger
  -> Batch job
     -> Step
        -> Partition
           -> sampled chunk
```

Do NOT create one trace span per record by default.

10M spans per job are operationally expensive and unnecessary.

---

# 49. Spring Boot Actuator

Expose at least:

```text
/actuator/health
/actuator/info
/actuator/metrics
/actuator/prometheus
```

Actual exposure MUST follow security policy.

Health SHOULD distinguish:

- liveness,
- readiness,
- PostgreSQL connectivity,
- optional input-storage readiness.

Do not make liveness depend on transient external dependencies in a way that creates restart storms.

---

# 50. Security Requirements

The application MUST:

- run as a non-root container user,
- use read-only filesystem where feasible,
- have write access only to required working directories,
- retrieve DB credentials from Kubernetes Secrets or an external secret manager,
- never log credentials,
- validate file paths against traversal attacks,
- restrict accepted input locations,
- validate file size before processing,
- treat CSV fields as untrusted input,
- prevent formula-injection risks if generated CSV output is added later.

---

# 51. Kubernetes Deployment

Recommended components:

```text
Deployment:
  batch-scheduler-app
  replicas: 2

PostgreSQL:
  external managed service or HA stateful database

Persistent/object storage:
  input file source
```

Both pods can contain Quartz and Spring Batch runtime.

Quartz clustering ensures scheduling HA.

Only one logical JobInstance may process a file at a time.

---

# 52. Kubernetes Resource Strategy

Start with explicit resource requests and limits.

Example placeholders:

```yaml
resources:
  requests:
    cpu: "2"
    memory: "2Gi"
  limits:
    cpu: "4"
    memory: "4Gi"
```

These values are NOT production recommendations.

They MUST be replaced using load-test evidence.

The JVM MUST be container-aware.

Heap sizing MUST leave room for:

- native memory,
- thread stacks,
- direct buffers,
- memory-mapped/file buffers if used,
- observability agent overhead.

---

# 53. Kubernetes Graceful Shutdown

The application MUST handle SIGTERM correctly.

When shutdown begins:

1. stop accepting new scheduled launches,
2. allow current checkpoint/transaction to finish when possible,
3. persist Spring Batch state,
4. terminate within Kubernetes grace period.

Configure:

```text
terminationGracePeriodSeconds
```

based on maximum safe chunk completion time.

Do not set it arbitrarily low.

---

# 54. Rolling Deployments

A rolling deployment MUST NOT accidentally start two copies of the same JobInstance.

Protection relies on:

- Quartz clustering,
- `@DisallowConcurrentExecution`,
- Spring Batch JobRepository,
- deterministic JobParameters,
- manifest unique constraints.

This behavior MUST be tested.

---

# 55. Scheduler vs Kubernetes CronJob

Quartz is intentionally selected for this specification.

However, document the architectural alternative:

```text
Kubernetes CronJob
    -> starts Spring Batch workload
```

Use Quartz when:

- persistent scheduler state is desired,
- misfire behavior matters,
- schedule management is application-owned,
- clustered scheduler failover is required.

Do not simultaneously configure Kubernetes CronJob and Quartz to launch the same weekly job.

---

# 56. Performance Requirements

The system MUST define and test an SLA.

Suggested initial acceptance target for benchmarking:

```text
10,000,000 representative records
complete within 60 minutes
```

This is a baseline target, not an inferred business requirement.

If the actual SLA differs, update this specification before final tuning.

Performance tests MUST report:

```text
records/sec
CPU utilization
heap usage
GC pause time
disk throughput
DB load
executor queue depth
chunk latency p50/p95/p99
total job duration
```

---

# 57. Memory Requirements

Memory consumption MUST be approximately bounded by:

```text
chunk size
x number of in-flight chunks
x average record/result object size
+ reference data cache
+ framework/runtime overhead
```

Memory MUST NOT scale linearly with total file size.

For a 10M-record file, the application MUST NOT retain all processed records.

---

# 58. GC and Allocation Strategy

During performance tuning:

- inspect allocation rate,
- reduce unnecessary intermediate objects,
- use immutable compact records,
- avoid repeated regex compilation,
- avoid repeated formatter construction,
- reuse thread-safe parsers/helpers where safe,
- avoid collecting large streams into lists.

Do not introduce unsafe object pooling prematurely.

---

# 59. Database Load Strategy

Spring Batch metadata writes occur at chunk/checkpoint boundaries.

Chunk size affects:

- restart granularity,
- DB metadata frequency,
- transaction size,
- memory,
- throughput.

This tradeoff MUST be measured.

Do not choose an extremely small chunk size without evidence.

---

# 60. Failover Scenarios

The implementation MUST be tested against at least:

## Scenario A — JVM crash mid-job

Expected:

```text
job FAILED
checkpoint preserved
restart succeeds
```

## Scenario B — PostgreSQL unavailable temporarily

Expected:

```text
bounded retry where safe
job fails if metadata cannot be persisted
restart after DB recovery
```

## Scenario C — Input storage temporarily unavailable

Expected:

```text
validation or read failure
job fails cleanly
restart after storage recovery
```

## Scenario D — Scheduler pod dies before firing

Expected:

```text
another Quartz cluster node fires or handles recovery
```

## Scenario E — Scheduled time missed

Expected:

```text
configured misfire policy applied
```

## Scenario F — Pod SIGTERM during processing

Expected:

```text
graceful checkpoint when possible
restartable execution
```

## Scenario G — Duplicate scheduler launch attempt

Expected:

```text
second launch rejected
no duplicate processing
```

---

# 61. Testing Strategy

## 61.1 Unit Tests

Test:

- CSV parsing,
- domain validation,
- classification rules,
- file identity generation,
- checksum logic,
- manifest transitions,
- retry classifier,
- skip classifier,
- reconciliation arithmetic.

Target deterministic, fast tests.

---

## 61.2 Spring Batch Tests

Use Spring Batch testing support.

Test:

- individual tasklets,
- individual chunk step,
- job flow,
- execution context,
- restart behavior,
- skip behavior,
- retry behavior,
- listener behavior.

---

## 61.3 PostgreSQL Integration Tests

Use Testcontainers PostgreSQL.

Tests MUST validate:

- Flyway migrations,
- Quartz schema,
- Spring Batch schema,
- manifest constraints,
- JobRepository persistence,
- restart after recreated Spring application context.

Do not use H2 as a substitute for PostgreSQL-specific integration behavior.

---

## 61.4 Quartz Integration Tests

Verify:

- trigger creation,
- timezone,
- cron,
- misfire policy,
- persistent JobStore,
- `@DisallowConcurrentExecution`,
- cluster behavior where practical.

At minimum, test duplicate firing protection using two application scheduler instances against one Testcontainers PostgreSQL.

---

## 61.5 CSV Scale Tests

Generate representative files:

```text
100K
1M
10M
20M
```

The 10M dataset MUST be included in release-performance testing.

Test realistic:

- field lengths,
- quoted fields,
- malformed rows,
- multiline fields if allowed,
- unicode,
- null/empty values,
- extreme numeric/date values.

---

## 61.6 Restart Tests

Inject failures at deterministic record/chunk boundaries.

Example:

```text
throw after logical record 5,432,100
```

Then restart the same JobInstance.

Assert:

- job resumes correctly,
- completed work is not incorrectly counted twice,
- final reconciliation is correct.

---

## 61.7 Concurrency Tests

Test:

- processor thread safety,
- writer thread safety,
- bounded executor behavior,
- queue saturation,
- no deadlock,
- no unbounded memory growth.

---

## 61.8 Performance Tests

Benchmark matrix:

```text
chunk:
500, 1000, 2000, 5000, 10000

workers:
1, 2, 4, 8, 16 as hardware allows

thread model:
platform
virtual where applicable
```

Record all metrics.

Select defaults based on evidence.

---

# 62. Benchmark Decision Process

Tune in this order:

1. sequential baseline,
2. chunk size,
3. parser efficiency,
4. processor CPU cost,
5. reference-data access,
6. number of concurrent chunk workers,
7. DB checkpoint overhead,
8. only then consider file partitioning,
9. only then consider remote partitioning.

Do not start by maximizing thread count.

---

# 63. Profiling

Performance investigation SHOULD include:

- Java Flight Recorder,
- async-profiler or equivalent,
- GC logs,
- Micrometer metrics,
- PostgreSQL query/connection metrics,
- filesystem/object-storage throughput.

Optimization decisions MUST be attached to measured evidence.

---

# 64. Acceptance Criteria

The implementation is accepted only if all relevant criteria pass.

## AC-001

The scheduled trigger fires every Monday at the configured local time and timezone.

## AC-002

Two scheduler replicas sharing PostgreSQL do not create duplicate logical job executions.

## AC-003

A missed schedule follows the configured Quartz misfire policy.

## AC-004

A representative 10M-record CSV is processed without loading the entire file into memory.

## AC-005

The process stays within configured memory limits during the 10M-record test.

## AC-006

No unbounded executor/task queue exists in the batch processing path.

## AC-007

The system does not create one thread per record.

## AC-008

The processing job can be terminated mid-file and restarted from persistent Spring Batch state.

## AC-009

Restart produces correct final counts.

## AC-010

A completed file cannot be accidentally processed again using the same deterministic job identity.

## AC-011

Invalid records follow configured skip/fail policy.

## AC-012

Infrastructure failures do not get silently classified as business rejections.

## AC-013

Per-record successful logging is absent.

## AC-014

Prometheus-compatible batch metrics are available.

## AC-015

Logs can correlate a Quartz trigger, Batch JobExecution, StepExecution, and file checksum.

## AC-016

Flyway creates all required production database schemas/tables.

## AC-017

Production configuration disables automatic destructive/reinitializing framework schema behavior.

## AC-018

Performance benchmark results include throughput, CPU, memory, GC, and executor queue depth.

## AC-019

The chosen default chunk size and worker count are documented with benchmark evidence.

## AC-020

The core reader/processor architecture is independent from any message broker.

---

# 65. Architecture Decision Records

The repository MUST contain ADRs for major decisions.

Recommended:

```text
docs/adr/

0001-use-quartz-for-application-owned-scheduling.md
0002-use-postgresql-for-quartz-and-batch-metadata.md
0003-use-single-sequential-reader-for-v1.md
0004-use-bounded-platform-thread-executor.md
0005-do-not-partition-csv-by-naive-line-number.md
0006-use-deterministic-file-identity.md
0007-use-chunk-checkpoints-for-restartability.md
0008-use-downstream-neutral-result-sink.md
0009-scale-by-measurement-before-remote-partitioning.md
```

---

# 66. Key ADR: Single Reader First

Decision:

> Start with one sequential reader and bounded concurrent processing.

Reasons:

- safest CSV semantics,
- no duplicated scans,
- simplest restart model,
- easiest correctness validation,
- sequential file I/O is efficient,
- 10M records may already meet the SLA without distributed complexity.

Revisit if:

```text
reader utilization is proven bottleneck
AND
processing workers are consistently starved
```

---

# 67. Key ADR: Platform Threads by Default

Decision:

> Use a bounded platform-thread executor for the CPU-oriented processing path.

Reasons:

- CSV parsing and business classification are often CPU-heavy,
- CPU throughput remains limited by processor cores,
- bounded concurrency provides predictable resource usage.

Virtual threads may be introduced for blocking helper workloads after benchmark evidence.

---

# 68. Key ADR: PostgreSQL for Both Framework Metadata Stores

Decision:

```text
Quartz -> PostgreSQL
Spring Batch -> PostgreSQL
```

Reasons:

- persistence,
- scheduler clustering,
- restartability,
- operational visibility,
- transactional metadata,
- mature Spring integration.

Separate schemas prevent ownership confusion.

---

# 69. Implementation Phases

AI agents MUST implement in the following order.

## Phase 0 — Bootstrap

Deliver:

- Spring Boot 4 project,
- Java 25 toolchain,
- dependency management,
- configuration properties,
- PostgreSQL Testcontainers,
- Flyway,
- Actuator,
- baseline CI.

Exit criteria:

```text
application starts
migrations pass
health is UP
tests run in CI
```

---

## Phase 1 — Persistence and Control Plane

Implement:

- PostgreSQL schemas,
- Quartz tables,
- Spring Batch tables,
- `batch_file_manifest`,
- repositories,
- manifest state machine.

Tests:

- migration tests,
- uniqueness tests,
- lifecycle tests.

---

## Phase 2 — Quartz Scheduler

Implement:

- durable weekly trigger,
- timezone,
- misfire policy,
- Quartz JDBC store,
- cluster configuration,
- launch service,
- concurrent-execution protection.

Tests:

- cron,
- misfire,
- duplicate launch,
- restart with persisted trigger.

---

## Phase 3 — Input Resolution and Validation

Implement:

- `InputFileResolver`,
- immutable input handling,
- checksum,
- CSV header/schema validation,
- manifest registration,
- `validateInputStep`.

Tests:

- missing file,
- empty file,
- invalid header,
- duplicate checksum,
- changed file,
- valid file.

---

## Phase 4 — Baseline Sequential Batch

Implement:

```text
reader
processor
lightweight writer
reconciliation
finalize
```

Use one processing thread first.

Tests:

- 100K records,
- parsing,
- classification,
- counters,
- restart.

This phase establishes the correctness baseline.

---

## Phase 5 — Bounded Concurrent Processing

Introduce dedicated batch executor.

Requirements:

- bounded pool,
- bounded queue,
- controlled in-flight chunks,
- thread-safe processor/writer,
- measurable executor metrics.

Benchmark against Phase 4.

Do not merge if correctness or restartability regresses.

---

## Phase 6 — Observability

Implement:

- Micrometer metrics,
- structured logging,
- progress reporting,
- job/step listeners,
- tracing,
- dashboards/alert recommendations.

---

## Phase 7 — 10M Performance Qualification

Run representative 10M workload.

Benchmark matrix:

```text
chunk sizes
worker counts
CPU allocations
memory allocations
```

Document chosen defaults.

Exit criteria:

- SLA achieved or bottleneck clearly identified,
- memory bounded,
- no queue explosion,
- restart test passes at scale.

---

## Phase 8 — Optional Local Partitioning

Implement only if performance evidence requires it.

Before implementation, confirm CSV record-boundary contract.

Preferred safe strategies:

- indexed record boundaries,
- upstream shards.

Naive newline splitting is prohibited unless input contract proves it safe.

---

## Phase 9 — Optional Remote Partitioning

Implement only when one-node/local-partition processing cannot meet required SLA or operational isolation requirements.

Worker pods MUST be independently restartable.

Do not add distributed processing without an explicit performance/availability justification.

---

# 70. CI/CD Gates

A pull request MUST pass:

```text
compile
format/lint
unit tests
integration tests
migration tests
Spring Batch tests
security/static analysis as configured
```

Release candidate MUST additionally pass:

```text
restart/failure tests
performance smoke test
10M qualification benchmark when performance-sensitive code changed
```

---

# 71. Code Quality Rules for AI Agents

AI coding agents MUST:

1. Prefer explicit, readable code over framework magic.
2. Use constructor injection.
3. Avoid field injection.
4. Use immutable domain models.
5. Keep scheduling code separate from processing code.
6. Keep batch configuration separate from business rules.
7. Avoid static mutable state.
8. Avoid hidden global executors.
9. Avoid blocking calls inside processors unless documented.
10. Add tests with every batch behavior change.
11. Add benchmark evidence before increasing concurrency defaults.
12. Never disable restartability merely to simplify implementation.
13. Never replace PostgreSQL integration tests with H2.
14. Never log all records.
15. Never use `parallelStream()` for the core batch pipeline.
16. Never use an unbounded executor queue.
17. Never introduce naive CSV partitioning.
18. Never couple the processor directly to future messaging infrastructure.

---

# 72. Prohibited Anti-Patterns

The following designs are explicitly forbidden.

## Anti-pattern 1

```java
@Scheduled
void processTenMillionRows() {
    Files.lines(path).forEach(...);
}
```

Reason:

- weak restartability,
- poor checkpoint control,
- scheduler owns business processing.

## Anti-pattern 2

```java
records.parallelStream().forEach(...)
```

Reason:

- poor executor control,
- weak backpressure,
- difficult checkpoint/restart semantics.

## Anti-pattern 3

```text
10M records -> 10M tasks -> unbounded queue
```

Reason:

- memory risk,
- latency explosion,
- poor backpressure.

## Anti-pattern 4

```text
10M records -> 10M threads
```

Reason:

- unnecessary and unsafe concurrency.

## Anti-pattern 5

Per-record DB or REST lookup without proof it is necessary.

## Anti-pattern 6

Multiple readers each rescanning the file from the beginning to reach their assigned line range.

## Anti-pattern 7

Splitting arbitrary CSV by newline without proving multiline fields are impossible.

## Anti-pattern 8

Auto-creating production Quartz/Spring Batch schemas on every startup.

---

# 73. Operational Runbook Requirements

Create:

```text
docs/runbook/batch-operations.md
```

It MUST explain:

- how to identify current job status,
- how to find `JobExecution`,
- how to find `StepExecution`,
- how to identify the input checksum,
- how to distinguish failed vs completed files,
- how to restart a failed job,
- when NOT to restart,
- how to handle corrupted input,
- how to handle a missed Monday schedule,
- how to inspect Quartz trigger state,
- how to investigate slow throughput,
- how to verify executor saturation,
- how to verify PostgreSQL health.

---

# 74. Alerting Recommendations

Create alerts for:

```text
weekly job did not start in expected window
weekly job failed
job duration exceeds SLA
records/sec below baseline for sustained period
invalid record ratio above threshold
executor queue remains saturated
PostgreSQL unavailable
Quartz trigger misfire
duplicate launch attempt
reconciliation failure
```

Do not page on ordinary business rejections.

---

# 75. Future Message-Queue Extension Point

Message queues are out of scope, but the architecture MUST preserve this seam:

```text
CsvRecordReader
      |
      v
CsvRecordProcessor
      |
      v
ProcessingResult
      |
      v
ProcessingResultSink
```

Current:

```text
ProcessingResultSink
    -> ClassificationMetricsWriter
```

Future:

```text
ProcessingResultSink
    -> Kafka/Rabbit/etc.
```

The future sink MUST be replaceable without redesigning:

- Quartz,
- JobParameters,
- file identity,
- reader,
- processor,
- checkpoint model,
- manifest lifecycle,
- core observability.

No broker-specific type may appear in domain-processing APIs in this version.

---

# 76. Definition of Done

The architecture is considered implemented when:

1. Two Kubernetes replicas can run with clustered Quartz.
2. One Monday trigger launches exactly one logical Spring Batch job.
3. The job validates a deterministic immutable CSV input.
4. The job processes 10M representative records in streaming mode.
5. Resource consumption remains bounded.
6. A forced mid-job failure can be restarted correctly.
7. Final counts reconcile.
8. Completed files cannot be silently rerun.
9. Metrics and structured logs provide operational visibility.
10. All production database schemas are migration-managed.
11. No message broker is required.
12. The downstream sink is replaceable.
13. Performance defaults are supported by benchmark evidence.
14. All mandatory automated tests pass.

---

# 77. Reference Execution Flow

```text
Monday 06:00 Asia/Ho_Chi_Minh
              |
              v
      Quartz CronTrigger
              |
              v
      WeeklyCsvQuartzJob
              |
              v
       resolve input file
              |
              v
 construct deterministic parameters
              |
              v
      Spring Batch Job
              |
              v
      validateInputStep
              |
              v
    loadReferenceDataStep
              |
              v
       processCsvStep
              |
      sequential streaming read
              |
      configurable chunk size
              |
      bounded task executor
              |
      deterministic classifier
              |
      result metrics sink
              |
              v
        reconcileStep
              |
              v
         finalizeStep
              |
              v
          COMPLETED
```

Failure path:

```text
processCsvStep
      |
      X
 infrastructure / process failure
      |
      v
    FAILED
      |
  state persisted
      |
 application recovers
      |
 restart same JobInstance
      |
 continue from checkpoint
      |
 reconciliation
      |
   COMPLETED
```

---

# 78. Final Engineering Principle

The implementation must optimize for the following order:

```text
Correctness
    >
Restartability
    >
Bounded resource usage
    >
Observability
    >
Measured throughput
    >
Architectural complexity
```

Do not introduce distributed processing, virtual-thread fan-out, or aggressive partitioning merely because the file contains 10M records.

A 10M-record workload is a reason to **measure and design carefully**, not automatically a reason to distribute the system.

The preferred evolution is:

```text
Sequential baseline
    ->
Chunk tuning
    ->
Bounded concurrent processing
    ->
Profile
    ->
Safe local partitioning if required
    ->
Upstream sharding if available
    ->
Remote partitioning only if justified
```

This order is mandatory unless production measurements provide a documented reason to deviate.
