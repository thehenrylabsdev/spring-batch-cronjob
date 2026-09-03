package dev.thehenrylabs.springbatchcronjob.repository;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;

import dev.thehenrylabs.springbatchcronjob.domain.BatchRunSummary;

/** Spring Data JDBC repository for {@code control.batch_run_summary} (SPEC 27, 40). */
public interface BatchRunSummaryRepository extends CrudRepository<BatchRunSummary, Long> {

	Optional<BatchRunSummary> findByBusinessDateAndFileChecksum(LocalDate businessDate, String fileChecksum);

	Optional<BatchRunSummary> findByRunId(String runId);

	/**
	 * Finalize the summary row with reconciliation results; guarded on current status.
	 */
	@Modifying
	@Query("""
			UPDATE control.batch_run_summary
			SET finished_at = :finishedAt, duration_ms = :durationMs, status = :to, updated_at = :at
			WHERE run_id = :runId AND status = :from
			""")
	int finalizeSummary(String runId, String from, String to, Instant finishedAt, Long durationMs, Instant at);

}
