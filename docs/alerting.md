# Alerting Recommendations (SPEC 74)

| Alert condition | Metric / signal | Severity |
|---|---|---|
| Weekly job did not start in expected window | `quartz_trigger_fired_total` delta over the Monday window | page |
| Weekly job failed | sum(rate(`batch_job_failed_total`[5m])) > 0 | page |
| Job duration exceeds SLA | `batch_job_duration_seconds` > alert threshold (60 min baseline) | page |
| records/sec below baseline sustained | `batch_records_per_second` < 20% of baseline for 10m | page |
| Invalid record ratio above threshold | `batch_records_invalid_total` / `batch_records_read_total` > 5% | ticket |
| Executor queue remains saturated | `batch_executor_queue_depth` >= capacity for 10m | ticket |
| PostgreSQL unavailable | `up` on postgres DB probe / health READY | page |
| Quartz trigger misfire | delta(`quartz_trigger_misfire_total`) > 0 | ticket |
| Duplicate launch attempt | delta(`batch_duplicate_launch_prevented_total`) > 0 | ticket |
| Reconciliation failure | `BatchInvariantException` in logs / job failed | page |

Do NOT page on ordinary business rejections: `batch_records_rejected_total` below the
ratio threshold is normal business signal.
