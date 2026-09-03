package dev.thehenrylabs.springbatchcronjob.job.processing;

import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.domain.BatchFileManifest;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;

/**
 * processCsvStep lifecycle glue: publishes the input file URI into the step execution
 * context (read by the reader's ItemStream.open), restores restart-safe counters, and
 * moves the manifest READY -> PROCESSING.
 */
@Component
public class ProcessingStepListener implements StepExecutionListener {

	private final FileManifestService manifestService;

	private final ClassificationMetricsWriter metricsWriter;

	public ProcessingStepListener(FileManifestService manifestService, ClassificationMetricsWriter metricsWriter) {
		this.manifestService = manifestService;
		this.metricsWriter = metricsWriter;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		String fileUri = stepExecution.getJobParameters().getString(BatchLaunchService.PARAM_FILE_URI);
		String checksum = stepExecution.getJobParameters().getString(BatchLaunchService.PARAM_FILE_CHECKSUM);
		String businessDate = stepExecution.getJobParameters().getString(BatchLaunchService.PARAM_BUSINESS_DATE);

		stepExecution.getExecutionContext().putString(CsvRecordReader.KEY_FILE_URI, fileUri);

		BatchFileManifest manifest = this.manifestService
			.findByIdentity(java.time.LocalDate.parse(businessDate), checksum)
			.orElseThrow(() -> new BatchInvariantException(
					"No manifest found for " + businessDate + "/" + checksum + " — launcher was bypassed"));
		this.manifestService.transitionToProcessing(manifest.id());

		this.metricsWriter.reset();
		this.metricsWriter.restore(stepExecution.getExecutionContext());
	}

}
