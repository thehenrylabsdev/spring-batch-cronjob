package dev.thehenrylabs.springbatchcronjob.domain;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

/**
 * Business-facing run summary row (SPEC 27, 40). Counters must satisfy the reconciliation
 * equation before the row is finalized as {@link RunSummaryStatus#COMPLETED}.
 *
 * @param id database identity
 * @param runId stable run identifier (e.g. Spring Batch job execution id)
 * @param businessDate weekly run date (Monday)
 * @param fileChecksum SHA-256 hex digest of the immutable input
 * @param readCount records read
 * @param acceptedCount records accepted by the classifier
 * @param rejectedCount business rejections
 * @param invalidCount records invalid under the CSV/domain contract
 * @param skippedCount records skipped by the skip policy
 * @param startedAt processing start time
 * @param finishedAt processing finish time
 * @param durationMs total duration
 * @param status run state
 * @param createdAt row creation time
 * @param updatedAt last modification time
 */
@Table(name = "batch_run_summary", schema = "control")
public record BatchRunSummary(@Id Long id, String runId, LocalDate businessDate, String fileChecksum, long readCount,
		long acceptedCount, long rejectedCount, long invalidCount, long skippedCount, Instant startedAt,
		Instant finishedAt, Long durationMs, RunSummaryStatus status, Instant createdAt, Instant updatedAt) {

	/** Convenience accessor for the terminal duration. */
	public Duration duration() {
		if (this.startedAt == null || this.finishedAt == null) {
			return null;
		}
		return Duration.between(this.startedAt, this.finishedAt);
	}

}
