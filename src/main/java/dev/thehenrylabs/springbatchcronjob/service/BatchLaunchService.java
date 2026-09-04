package dev.thehenrylabs.springbatchcronjob.service;

import java.net.URI;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.stereotype.Service;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;
import dev.thehenrylabs.springbatchcronjob.file.FileChecksumService;
import dev.thehenrylabs.springbatchcronjob.file.ImmutableInputService;
import dev.thehenrylabs.springbatchcronjob.file.InputFileResolver;
import dev.thehenrylabs.springbatchcronjob.file.ResolvedInputFile;
import dev.thehenrylabs.springbatchcronjob.file.SeekableInputResourceResolver;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Deterministic job identity and the launch guard (SPEC 11, 39).
 *
 * <p>
 * The identity is {@code (businessDate, fileChecksum, fileUri)}: the same logical file
 * always maps to the same {@link JobParameters} and therefore the same Spring Batch
 * {@code JobInstance}. Completed instances cannot be silently rerun, running instances
 * cannot be double-launched, failed instances remain restartable.
 */
@Service
public class BatchLaunchService {

	public static final String JOB_NAME = "weeklyCsvCoordinatorJob";

	public static final String PARAM_BUSINESS_DATE = "businessDate";

	public static final String PARAM_FILE_CHECKSUM = "fileChecksum";

	public static final String PARAM_FILE_URI = "fileUri";

	public static final String PARAM_FILE_LOGICAL_NAME = "fileLogicalName";

	public static final String PARAM_FILE_GENERATION = "fileGeneration";

	private static final Logger log = LoggerFactory.getLogger(BatchLaunchService.class);

	private final JobOperator jobOperator;

	private final JobRegistry jobRegistry;

	private final InputFileResolver inputFileResolver;

	private final FileManifestService manifestService;

	private final FileChecksumService checksumService;

	private final ImmutableInputService immutableInput;

	private final MeterRegistry meterRegistry;

	private final BatchProperties properties;

	private final SeekableInputResourceResolver seekableInput;

	public BatchLaunchService(JobOperator jobOperator, JobRegistry jobRegistry, InputFileResolver inputFileResolver,
			FileManifestService manifestService, FileChecksumService checksumService,
			ImmutableInputService immutableInput, MeterRegistry meterRegistry, BatchProperties properties,
			SeekableInputResourceResolver seekableInput) {
		this.jobOperator = jobOperator;
		this.jobRegistry = jobRegistry;
		this.inputFileResolver = inputFileResolver;
		this.manifestService = manifestService;
		this.checksumService = checksumService;
		this.immutableInput = immutableInput;
		this.meterRegistry = meterRegistry;
		this.properties = properties;
		this.seekableInput = seekableInput;
	}

	/** Outcome of a launch attempt (SPEC 39 layers). */
	public enum Outcome {

		STARTED, DUPLICATE_COMPLETED, DUPLICATE_RUNNING, DUPLICATE_MANIFEST, RESTARTABLE, ERROR

	}

	public record LaunchResult(Outcome outcome, Optional<Long> jobExecutionId, String detail) {

	}

	/**
	 * Resolve, register and launch the weekly job for a business date.
	 */
	public LaunchResult launchWeekly(LocalDate businessDate) {
		// The former directory convention remains a cutover-only compatibility path.
		// New deployments use the explicit shared resource contract exclusively.
		boolean legacyLocation = !"/data/incoming".equals(this.properties.input().location());
		ResolvedInputFile input = legacyLocation ? this.inputFileResolver.resolve(businessDate)
				: this.seekableInput.resolve();
		String checksum = resolveChecksum(input);
		// Production input is an immutable shared read-only generation. Moving/deleting
		// it
		// would make other pods observe different bytes, so process exactly this
		// resource.
		ResolvedInputFile processing = input;

		FileManifestService.Registration registration = this.manifestService.registerDiscovered(businessDate,
				processing.fileName(), processing.uri(), processing.sizeBytes(), checksum);
		if (registration.duplicate()) {
			duplicate("duplicate manifest identity " + businessDate + "/" + checksum);
			return new LaunchResult(Outcome.DUPLICATE_MANIFEST, Optional.empty(),
					"file already registered (manifest row " + registration.manifest().id() + ")");
		}
		JobParametersBuilder parameterBuilder = new JobParametersBuilder()
			.addString(PARAM_BUSINESS_DATE, businessDate.toString())
			.addString(PARAM_FILE_CHECKSUM, checksum)
			.addString(PARAM_FILE_URI, processing.uri().toString());
		if (!legacyLocation) {
			parameterBuilder.addString(PARAM_FILE_LOGICAL_NAME, processing.fileName())
				.addString(PARAM_FILE_GENERATION, checksum);
		}
		JobParameters parameters = parameterBuilder.toJobParameters();

		log.info("Launching weekly CSV job businessDate={} file={} checksum={}", businessDate, input.fileName(),
				checksum);
		Job job = this.jobRegistry.getJob(JOB_NAME);
		if (job == null) {
			// Job bean missing from the registry — programming error, fail fast (SPEC
			// 34.4).
			throw new BatchInvariantException("Batch job '" + JOB_NAME + "' is not registered");
		}
		try {
			JobExecution execution = this.jobOperator.start(job, parameters);
			return new LaunchResult(Outcome.STARTED, Optional.of(execution.getId()),
					"job execution " + execution.getId());
		}
		catch (JobExecutionAlreadyRunningException running) {
			duplicate("running job instance for " + businessDate + "/" + checksum);
			return new LaunchResult(Outcome.DUPLICATE_RUNNING, Optional.empty(), running.getMessage());
		}
		catch (JobInstanceAlreadyCompleteException completed) {
			duplicate("completed job instance for " + businessDate + "/" + checksum);
			return new LaunchResult(Outcome.DUPLICATE_COMPLETED, Optional.empty(), completed.getMessage());
		}
		catch (JobRestartException restartable) {
			return new LaunchResult(Outcome.RESTARTABLE, Optional.empty(), restartable.getMessage());
		}
		catch (Exception e) {
			log.error("Launch failed for businessDate={} file={}", businessDate, input.fileName(), e);
			return new LaunchResult(Outcome.ERROR, Optional.empty(), e.getMessage());
		}
	}

	/**
	 * The checksum is computed exactly once per logical file, at the ingestion boundary
	 * (this launcher), never inside the processing job (SPEC 19). Assigning the checksum
	 * before launching keeps the Spring Batch identity deterministic.
	 *
	 * <p>
	 * When {@code batch.input.expected-checksum} is configured it is still computed and
	 * verified — a tampered file must not sail under a producer-claimed digest.
	 */
	private String resolveChecksum(ResolvedInputFile input) {
		String configured = this.properties.input().expectedChecksum();
		if (configured != null && !configured.isBlank()) {
			String actual = computeChecksum(input);
			if (!actual.equalsIgnoreCase(configured)) {
				throw new InvalidInputFileException("Input checksum mismatch: expected " + configured + " but was "
						+ actual + " for " + input.uri());
			}
			return actual;
		}
		if (input.checksum() != null && !input.checksum().isBlank()) {
			return input.checksum().toLowerCase();
		}
		return computeChecksum(input);
	}

	private String computeChecksum(ResolvedInputFile input) {
		URI uri = input.uri();
		if ("file".equals(uri.getScheme())) {
			return this.checksumService.sha256(Path.of(uri));
		}
		throw new InvalidInputFileException("Checksum must be supplied by the producer for non-file input: " + uri);
	}

	private void duplicate(String message) {
		log.warn("Duplicate weekly launch prevented: {}", message);
		this.meterRegistry.counter("batch_duplicate_launch_prevented_total").increment();
	}

}
