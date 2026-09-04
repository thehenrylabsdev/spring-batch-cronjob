package dev.thehenrylabs.springbatchcronjob.job.validation;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.domain.BatchFileManifest;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;
import dev.thehenrylabs.springbatchcronjob.file.CsvSchemaValidator;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;

/**
 * validateInputStep tasklet (SPEC 13, 19). Verifies the *immutable processing copy*
 * against the manifest and the CSV contract. A failure must stop the job before the
 * 10M-record phase begins; the manifest moves to FAILED and the job remains restartable.
 *
 * <p>
 * No full-file scan happens here: checksum was computed at ingestion (launcher), and the
 * schema validation probes only a bounded prefix.
 */
@Component
public class InputValidationTasklet implements Tasklet {

	private static final Logger log = LoggerFactory.getLogger(InputValidationTasklet.class);

	private final FileManifestService manifestService;

	private final CsvSchemaValidator schemaValidator;

	private final BatchProperties properties;

	public InputValidationTasklet(FileManifestService manifestService, CsvSchemaValidator schemaValidator,
			BatchProperties properties) {
		this.manifestService = manifestService;
		this.schemaValidator = schemaValidator;
		this.properties = properties;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
		JobParameters parameters = chunkContext.getStepContext().getStepExecution().getJobParameters();
		LocalDate businessDate = LocalDate.parse(parameters.getString(BatchLaunchService.PARAM_BUSINESS_DATE));
		String checksum = parameters.getString(BatchLaunchService.PARAM_FILE_CHECKSUM);
		String fileUri = parameters.getString(BatchLaunchService.PARAM_FILE_URI);

		BatchFileManifest manifest = this.manifestService.findByIdentity(businessDate, checksum)
			.orElseThrow(() -> new InvalidInputFileException("No manifest registered for identity " + businessDate + "/"
					+ checksum + " — job launched outside the launcher"));

		Long id = manifest.id();
		try {
			this.manifestService.transitionToValidating(id);
			validate(manifest, fileUri);
			BatchFileManifest ready = this.manifestService.transitionToIndexing(id);
			log.info("Input validated businessDate={} file={} checksum={} manifestId={}", businessDate,
					ready.fileName(), checksum, id);
			return RepeatStatus.FINISHED;
		}
		catch (RuntimeException e) {
			try {
				this.manifestService.transitionToFailed(id);
			}
			catch (RuntimeException stateError) {
				e.addSuppressed(stateError);
			}
			throw e;
		}
	}

	private void validate(BatchFileManifest manifest, String fileUri) {
		Path file = Path.of(URI.create(fileUri));
		if (!Files.isRegularFile(file)) {
			throw new InvalidInputFileException("Input file does not exist: " + file);
		}
		if (!Files.isReadable(file)) {
			throw new InvalidInputFileException("Input file is not readable: " + file);
		}
		long size = sizeOf(file);
		if (size <= 0) {
			throw new InvalidInputFileException("Input file is empty: " + file);
		}
		BatchProperties.Input input = this.properties.input();
		if (size < input.minSizeBytes()) {
			throw new InvalidInputFileException("Input file below minimum size " + input.minSizeBytes() + ": " + file);
		}
		if (size > input.maxSizeBytes()) {
			throw new InvalidInputFileException(
					"Input file exceeds maximum size " + input.maxSizeBytes() + ": " + file);
		}
		if (size != manifest.fileSizeBytes()) {
			throw new InvalidInputFileException("Input file size changed since discovery (" + manifest.fileSizeBytes()
					+ " -> " + size + "): " + file);
		}
		// The processing copy must not have been modified after the checksum was taken.
		java.time.Instant modified = lastModified(file);
		if ("/data/incoming".equals(input.location())
				&& modified.plus(input.stabilityWindow()).isAfter(java.time.Instant.now())) {
			throw new InvalidInputFileException(
					"Input has not satisfied stability window " + input.stabilityWindow() + ": " + file);
		}
		if (modified.isAfter(manifest.discoveredAt())) {
			throw new InvalidInputFileException("Input file modified after discovery (" + manifest.discoveredAt()
					+ " -> " + modified + "): " + file);
		}
		this.schemaValidator.validate(file, input);
	}

	private static long sizeOf(Path file) {
		try {
			return Files.size(file);
		}
		catch (java.io.IOException e) {
			throw new InvalidInputFileException("Cannot stat input file: " + file, e);
		}
	}

	private static java.time.Instant lastModified(Path file) {
		try {
			return Files.getLastModifiedTime(file).toInstant();
		}
		catch (java.io.IOException e) {
			throw new InvalidInputFileException("Cannot stat input file timestamp: " + file, e);
		}
	}

}
