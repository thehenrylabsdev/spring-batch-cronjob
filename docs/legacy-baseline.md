# Legacy baseline

Before SPEC_02 the application used one `CsvRecordReader` starting at byte zero and a bounded local executor.
Restarting parsed and discarded already committed records, so it was safe for multiline CSV but could not share one
file across pods. The baseline remains available only as code-history/reference; `weeklyCsvCoordinatorJob` is the
active topology. It performs one coordinator scan, then uses PostgreSQL leases and checkpoints for horizontal work.

The legacy path must not be scheduled with `batch.distributed.enabled=true`.
