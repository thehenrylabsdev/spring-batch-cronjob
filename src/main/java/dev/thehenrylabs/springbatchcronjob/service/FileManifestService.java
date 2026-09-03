package dev.thehenrylabs.springbatchcronjob.service;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import dev.thehenrylabs.springbatchcronjob.domain.BatchFileManifest;
import dev.thehenrylabs.springbatchcronjob.domain.ManifestStatus;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.repository.BatchFileManifestRepository;

/**
 * Single owner of the {@code control.batch_file_manifest} lifecycle (SPEC 12, 39, 41).
 *
 * <p>
 * The {@code (business_date, checksum_sha256)} unique constraint is the serialization
 * point for concurrent duplicate registration; the guarded {@code @Modifying} transitions
 * reject transitions from an unexpected state.
 */
@Service
public class FileManifestService {

	private final BatchFileManifestRepository repository;

	private final Clock clock;

	public FileManifestService(BatchFileManifestRepository repository, Clock clock) {
		this.repository = repository;
		this.clock = clock;
	}

	/** Result of a registration attempt against the unique identity. */
	public record Registration(BatchFileManifest manifest, boolean duplicate) {
	}

	/**
	 * Register a discovered file. On {@code (business_date, checksum_sha256)} conflict
	 * the existing row is returned and {@code duplicate} is {@code true} — the caller
	 * must not launch a second job.
	 *
	 * <p>
	 * The fast path checks the unique identity first; the race path (two replicas
	 * inserting concurrently) fails the INSERT with the unique constraint and is
	 * converted into the same duplicate result. The INSERT runs in its own transaction
	 * because a failed statement in PostgreSQL aborts the whole transaction — the
	 * follow-up lookup must run in a fresh one.
	 */
	public Registration registerDiscovered(LocalDate businessDate, String fileName, URI fileUri, long sizeBytes,
			String checksumSha256) {
		BatchFileManifest existing = this.repository.findByBusinessDateAndChecksumSha256(businessDate, checksumSha256)
			.orElse(null);
		if (existing != null) {
			return new Registration(existing, true);
		}
		Instant now = this.clock.instant();
		BatchFileManifest discovery = new BatchFileManifest(null, businessDate, fileName, fileUri.toString(), sizeBytes,
				checksumSha256, ManifestStatus.DISCOVERED, now, null, null, null, now, now);
		try {
			return new Registration(insertDiscovered(discovery), false);
		}
		catch (DataIntegrityViolationException conflict) {
			return new Registration(this.repository.findByBusinessDateAndChecksumSha256(businessDate, checksumSha256)
				.orElseThrow(
						() -> new BatchInvariantException("Manifest constraint conflict but no row found for identity "
								+ businessDate + "/" + checksumSha256, conflict)),
					true);
		}
	}

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	BatchFileManifest insertDiscovered(BatchFileManifest discovery) {
		return this.repository.save(discovery);
	}

	@Transactional
	public BatchFileManifest transitionToValidating(Long id) {
		return guard(id, ManifestStatus.VALIDATING,
				transition(id, ManifestStatus.DISCOVERED, ManifestStatus.VALIDATING));
	}

	@Transactional
	public BatchFileManifest transitionToReady(Long id) {
		return guard(id, ManifestStatus.READY, transition(id, ManifestStatus.VALIDATING, ManifestStatus.READY));
	}

	@Transactional
	public BatchFileManifest transitionToProcessing(Long id) {
		Instant now = this.clock.instant();
		int rows = this.repository.transitionToProcessing(id, ManifestStatus.READY.name(),
				ManifestStatus.PROCESSING.name(), now, now);
		return guard(id, ManifestStatus.PROCESSING, rows);
	}

	@Transactional
	public BatchFileManifest transitionToCompleted(Long id, Long recordCount) {
		Instant now = this.clock.instant();
		int rows = this.repository.transitionToTerminal(id, ManifestStatus.PROCESSING.name(),
				ManifestStatus.COMPLETED.name(), now, now, recordCount);
		return guard(id, ManifestStatus.COMPLETED, rows);
	}

	@Transactional
	public BatchFileManifest transitionToFailed(Long id) {
		Instant now = this.clock.instant();
		// A failure can emerge from the validation phase (VALIDATING, READY) or from
		// an interrupted processing phase (PROCESSING after a crash + restart).
		int rows = this.repository.transitionToTerminal(id, ManifestStatus.PROCESSING.name(),
				ManifestStatus.FAILED.name(), now, now, null);
		if (rows == 0) {
			rows = this.repository.transitionToTerminal(id, ManifestStatus.VALIDATING.name(),
					ManifestStatus.FAILED.name(), now, now, null);
		}
		if (rows == 0) {
			rows = this.repository.transitionToTerminal(id, ManifestStatus.READY.name(), ManifestStatus.FAILED.name(),
					now, now, null);
		}
		return guard(id, ManifestStatus.FAILED, rows);
	}

	@Transactional
	public BatchFileManifest transitionToQuarantined(Long id) {
		Instant now = this.clock.instant();
		int rows = this.repository.transitionToTerminal(id, ManifestStatus.PROCESSING.name(),
				ManifestStatus.QUARANTINED.name(), now, now, null);
		return guard(id, ManifestStatus.QUARANTINED, rows);
	}

	public Optional<BatchFileManifest> find(Long id) {
		return this.repository.findById(id);
	}

	public List<BatchFileManifest> findForBusinessDate(LocalDate businessDate) {
		return this.repository.findByBusinessDateOrderByDiscoveredAtDesc(businessDate);
	}

	private int transition(Long id, ManifestStatus from, ManifestStatus to) {
		return this.repository.transition(id, from.name(), to.name(), this.clock.instant());
	}

	private BatchFileManifest guard(Long id, ManifestStatus target, int rows) {
		if (rows != 1) {
			throw new BatchInvariantException("Manifest transition to " + target + " for id " + id + " matched " + rows
					+ " rows; current state does not allow the transition");
		}
		return this.repository.findById(id)
			.orElseThrow(() -> new BatchInvariantException("Manifest row " + id + " disappeared after transition"));
	}

}
