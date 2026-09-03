package dev.thehenrylabs.springbatchcronjob.job.finalize;

import java.time.Instant;
import java.time.LocalDate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.domain.BatchRunSummary;
import dev.thehenrylabs.springbatchcronjob.domain.ManifestStatus;
import dev.thehenrylabs.springbatchcronjob.domain.RunSummaryStatus;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.repository.BatchRunSummaryRepository;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;

/**
 * finalizeStep (SPEC 41): terminalize the multi-part state — manifest COMPLETED with
 * record count and timestamps, run summary COMPLETED with duration.
 */
@Component
public class FinalizeBatchTasklet implements Tasklet {

	private static final Logger log = LoggerFactory.getLogger(FinalizeBatchTasklet.class);

	private static final String PROCESS_STEP = "processCsvStep";

	private final FileManifestService manifestService;

	private final BatchRunSummaryRepository summaryRepository;

	public FinalizeBatchTasklet(FileManifestService manifestService, BatchRunSummaryRepository summaryRepository) {
		this.manifestService = manifestService;
		this.summaryRepository = summaryRepository;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
		StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
		JobParameters parameters = stepExecution.getJobParameters();
		LocalDate businessDate = LocalDate.parse(parameters.getString(BatchLaunchService.PARAM_BUSINESS_DATE));
		String checksum = parameters.getString(BatchLaunchService.PARAM_FILE_CHECKSUM);
		String runId = String.valueOf(stepExecution.getJobExecutionId());

		Long recordCount = processingRecordCount(stepExecution);
		BatchRunSummary summary = this.summaryRepository.findByRunId(runId)
			.orElseThrow(() -> new BatchInvariantException("Run summary missing for run " + runId));

		Long manifestId = this.manifestService.findByIdentity(businessDate, checksum)
			.orElseThrow(
					() -> new BatchInvariantException("Manifest missing for identity " + businessDate + "/" + checksum))
			.id();

		Instant now = Instant.now();
		Long durationMs = summary.startedAt() == null ? null
				: java.time.Duration.between(summary.startedAt(), now).toMillis();

		if (this.manifestService.transitionToCompleted(manifestId, recordCount).status() != ManifestStatus.COMPLETED) {
			throw new BatchInvariantException("Manifest did not reach COMPLETED for id " + manifestId);
		}
		int rows = this.summaryRepository.finalizeSummary(runId, RunSummaryStatus.RUNNING.name(),
				RunSummaryStatus.COMPLETED.name(), now, durationMs, now);
		if (rows != 1) {
			throw new BatchInvariantException("Summary finalization matched " + rows + " rows for run " + runId);
		}
		log.info("Finalized businessDate={} manifestId={} recordCount={} durationMs={}", businessDate, manifestId,
				recordCount, durationMs);
		return RepeatStatus.FINISHED;
	}

	private Long processingRecordCount(StepExecution stepExecution) {
		return stepExecution.getJobExecution()
			.getStepExecutions()
			.stream()
			.filter(step -> PROCESS_STEP.equals(step.getStepName()))
			.findFirst()
			// Absolute across executions (restart-safe checkpoint)
			.map(step -> step.getExecutionContext()
				.getLong(dev.thehenrylabs.springbatchcronjob.job.processing.CsvRecordReader.KEY_RECORD_NUMBER,
						step.getReadCount()))
			.orElseThrow(() -> new BatchInvariantException("processCsvStep execution not found for finalize"));
	}

}
