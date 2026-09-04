package dev.thehenrylabs.springbatchcronjob.repository;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;

import dev.thehenrylabs.springbatchcronjob.domain.BatchFileManifest;

/**
 * Spring Data JDBC repository for {@code control.batch_file_manifest}.
 *
 * <p>
 * All state transitions go through the guarded {@code @Modifying} statements below so a
 * concurrent transition (e.g. two scheduler replicas) cannot pass a stale intermediate
 * state — the row's previous status is part of the WHERE clause.
 */
public interface BatchFileManifestRepository extends CrudRepository<BatchFileManifest, Long> {

	Optional<BatchFileManifest> findByBusinessDateAndChecksumSha256(LocalDate businessDate, String checksumSha256);

	List<BatchFileManifest> findByBusinessDateOrderByDiscoveredAtDesc(LocalDate businessDate);

	/**
	 * Guarded transition; returns the number of matched rows (0 when the row is not in
	 * the expected {@code from} state).
	 */
	@Modifying
	@Query("""
			UPDATE control.batch_file_manifest
			SET status = :to, updated_at = :at
			WHERE id = :id AND status = :from
			""")
	int transition(Long id, String from, String to, Instant at);

	@Modifying
	@Query("""
			UPDATE control.batch_file_manifest
			SET status = :to, updated_at = :at, processing_started_at = :processingStartedAt
			WHERE id = :id AND status = :from
			""")
	int transitionToProcessing(Long id, String from, String to, Instant at, Instant processingStartedAt);

	@Modifying
	@Query("""
			UPDATE control.batch_file_manifest SET status = :to, index_started_at = :at, updated_at = :at
			WHERE id = :id AND status = :from
			""")
	int transitionToIndexing(Long id, String from, String to, Instant at);

	@Modifying
	@Query("""
			UPDATE control.batch_file_manifest SET status = :to, expected_record_count = :records,
			index_finished_at = :at, updated_at = :at WHERE id = :id AND status = :from
			""")
	int finishIndex(Long id, String from, String to, Long records, Instant at);

	/** Retry-safe transition from any of the supplied predecessor states. */
	@Modifying
	@Query("""
			UPDATE control.batch_file_manifest
			SET status = :to, updated_at = :at
			WHERE id = :id AND status IN (:fromStates)
			""")
	int transitionFromAny(Long id, String to, List<String> fromStates, Instant at);

	/** Retry-safe processing start (crash + restart resumes a FAILED manifest). */
	@Modifying
	@Query("""
			UPDATE control.batch_file_manifest
			SET status = :to, updated_at = :at, processing_started_at = :at
			WHERE id = :id AND status IN (:fromStates)
			""")
	int transitionToProcessingFromAny(Long id, String to, List<String> fromStates, Instant at);

	@Modifying
	@Query("""
			UPDATE control.batch_file_manifest
			SET status = :to, updated_at = :at, processing_finished_at = :finishedAt, record_count = :recordCount
			WHERE id = :id AND status = :from
			""")
	int transitionToTerminal(Long id, String from, String to, Instant at, Instant finishedAt, Long recordCount);

}
