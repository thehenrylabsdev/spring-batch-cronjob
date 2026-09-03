# Batch Operations Runbook (SPEC 73)

All queries assume the app's search_path (`batch,quartz,control`); schema-qualified names
are shown for clarity.

## Identify current job status

```sql
SELECT job_execution_id, status, create_time, start_time, end_time
FROM batch.batch_job_execution ORDER BY job_execution_id DESC LIMIT 10;

SELECT business_date, file_name, checksum_sha256, status, record_count
FROM control.batch_file_manifest ORDER BY business_date DESC LIMIT 10;

SELECT run_id, business_date, status, read_count, accepted_count,
       rejected_count, invalid_count, skipped_count, duration_ms
FROM control.batch_run_summary ORDER BY business_date DESC LIMIT 10;
```

## Find JobExecution / StepExecution for a run

```sql
SELECT se.step_execution_id, se.step_name, se.status, se.read_count, se.write_count,
       se.skip_count, se.commit_count, se.exit_message
FROM batch.batch_step_execution se
JOIN batch.batch_job_execution je ON je.job_execution_id = se.job_execution_id
WHERE je.job_execution_id = :id ORDER BY se.step_execution_id;
```

## Identify the input checksum

`SELECT checksum_sha256, file_uri FROM control.batch_file_manifest WHERE business_date = :date;`
or the job parameter: `fileChecksum` in `batch.batch_job_execution_params`.

## Failed vs completed files

- `control.batch_file_manifest.status` = COMPLETED | FAILED | QUARANTINED.
- Correlated `batch_run_summary.status` (RUNNING means reconcile did not finish).

## Restart a failed job

```sql
-- find the failed execution
SELECT job_execution_id FROM batch.batch_job_execution WHERE status='FAILED'
ORDER BY job_execution_id DESC LIMIT 1;
```

Then trigger a restart — the application restart path is:
`BatchLaunchService.restart` equivalent via JobOperator (kubectl exec + actuator is not
provided; use the launcher service or `JobOperator.restart`). Preconditions:

- the immutable processing copy still exists at the manifest `file_uri`,
- the manifest status is FAILED (it is re-tried automatically on restart),
- the identical JobParameters are reused (never launch with different parameters).

## When NOT to restart

- When the failure is a validation failure caused by corrupted input — quarantine
  (`QUARANTINED`) instead and re-deliver a fixed file.
- When a NEW weekly file has already begun processing (avoid overlapping work).
- When the manifest is already COMPLETED (`transitionToCompleted` is guarded).

## Handle corrupted input

1. Set the manifest to QUARANTINED (manual update is acceptable with an audit comment
   in `updated_at`/`updated_by`; application support can be added via a small SQL):
   ```sql
   UPDATE control.batch_file_manifest SET status='QUARANTINED', updated_at=now()
   WHERE business_date=:date AND checksum_sha256=:checksum AND status IN ('DISCOVERED','VALIDATING','READY','PROCESSING');
   ```
2. Re-deliver the corrected file under the same naming convention; the new checksum
   creates a new identity and a fresh run.

## Missed Monday schedule

- Check `quartz.qrtz_triggers.trigger_state` for the weekly trigger; if PAUSED → resume
  via the scheduler.
- `quartz_trigger_misfire_total` > 0 means the configured misfire policy applied
  (default FIRE_ONCE_NOW) — the job should have fired on recovery; confirm via
  `batch_job_execution` timestamps.

## Inspect Quartz trigger state

```sql
SELECT t.trigger_name, t.trigger_state, t.next_fire_time, t.misfire_instr
FROM quartz.qrtz_triggers t WHERE t.trigger_group = 'weekly';
SELECT * FROM quartz.qrtz_scheduler_state; -- cluster members
```

## Investigate slow throughput

- `batch_records_per_second` gauge; compare with docs/benchmarks/defaults.md baseline.
- `batch_chunk_duration_seconds`, `batch_reader_latency_seconds` histograms.
- JFR + GC logs for allocation pressure (SPEC 63). Check the skip ratio first:
  `batch_records_invalid_total` — a corrupted run mixes INVALID rows into throughput.

## Verify executor saturation

- `batch_executor_active_threads`, `batch_executor_queue_depth`:
  sustained queue depth ≈ queue-capacity for minutes = workers cannot keep up →
  check processor cost or DB checkpoint latency; do NOT increase threads without a
  benchmark (SPEC 62).

## Verify PostgreSQL health

- `/actuator/health`, `/actuator/health/readiness`; `batch` connection-pool metrics via
  `/actuator/prometheus` (hikari pool gauges).
- Long-running chunk transactions: `SELECT * FROM pg_stat_activity WHERE state='active';`

## Shutdown notes (SPEC 53)

- `server.shutdown: graceful`, `spring.lifecycle.timeout-per-shutdown-phase: 90s` and
  `terminationGracePeriodSeconds` must exceed worst-case chunk time; the SIGTERM path
  lets the current checkpoint transaction finish, then Spring Batch state persists.
- Rolling deploys may not start a duplicate instance: protected by Quartz clustering +
  `@DisallowConcurrentExecution`, deterministic job parameters, manifest uniqueness —
  verified by `QuartzClusterFiringTests` and restart tests.
