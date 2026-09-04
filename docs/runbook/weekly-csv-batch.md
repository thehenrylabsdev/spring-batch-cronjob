# Weekly distributed CSV batch runbook

The input must be atomically published to the shared, read-only mount before Monday and must remain immutable until
the run completes. Configure `BATCH_INPUT_RESOURCE=file:/data/weekly/weekly.csv` on every pod.

Inspect workers:

```sql
SELECT worker_id,status,worker_slots,last_heartbeat_at,current_partition_id
FROM control.batch_worker ORDER BY last_heartbeat_at DESC;
```

Inspect a generation and its work:

```sql
SELECT id,status,expected_record_count,checksum_sha256 FROM control.batch_file_manifest ORDER BY id DESC;
SELECT partition_no,status,worker_id,lease_version,lease_expires_at,checkpoint_record_no,expected_record_count
FROM control.batch_partition WHERE manifest_id = :manifest_id ORDER BY partition_no;
```

Expired `CLAIMED` or `RUNNING` leases are automatically requeued. Do not manually change a live partition owner.
For graceful scale-down, reduce replicas and wait for `DRAINING` workers to release their current range. To stop new
runs set `BATCH_SCHEDULE_ENABLED=false`; retain `batch`, `control`, and `quartz` schemas during rollback. Never
enable the legacy scheduler while a distributed manifest is `INDEXING`, `READY`, or `PROCESSING`.

Investigate a failed reconciliation by comparing partition expected/processed counts and contiguous byte/record
ranges. A changed input file is a terminal failure: publish a new immutable generation rather than editing it.
