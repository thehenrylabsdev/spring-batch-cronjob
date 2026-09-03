package dev.thehenrylabs.springbatchcronjob.job.reconciliation;

import java.time.LocalDate;
import java.util.Map;

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
import dev.thehenrylabs.springbatchcronjob.domain.RunSummaryStatus;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.job.processing.ClassificationMetricsWriter;
import dev.thehenrylabs.springbatchcronjob.repository.BatchRunSummaryRepository;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * reconcileStep (SPEC 40): the classification counters must explain the read count.
 * {@code readCount == accepted + rejectedRuleA + rejectedRuleB + invalid + skipped}. Any
 * unexplained count fails the job (BatchInvariantException); a RUNNING summary row is
 * created/updated with the verified counts.
 */
@Component
public class ReconciliationTasklet implements Tasklet {

	private static final Logger log = LoggerFactory.getLogger(ReconciliationTasklet.class);

	private final BatchRunSummaryRepository summaryRepository;

	public ReconciliationTasklet(BatchRunSummaryRepository summaryRepository) {
		this.summaryRepository = summaryRepository;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
		StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
		JobParameters parameters = stepExecution.getJobParameters();
		LocalDate businessDate = LocalDate.parse(parameters.getString(BatchLaunchService.PARAM_BUSINESS_DATE));
		String checksum = parameters.getString(BatchLaunchService.PARAM_FILE_CHECKSUM);
		String runId = String.valueOf(stepExecution.getJobExecutionId());

		StepExecution processingStep = findProcessingStep(stepExecution);
		Map<String, Long> counts = statusCounts(processingStep);
		long account = counts.values().stream().mapToLong(Long::longValue).sum();
		// The reader checkpoint is ABSOLUTE across executions (restart-safe), so the
		// equation is: absoluteRead = sum(absolute outcome counts).
		long read = processingStep.getExecutionContext()
			.getLong(dev.thehenrylabs.springbatchcronjob.job.processing.CsvRecordReader.KEY_RECORD_NUMBER,
					processingStep.getReadCount());
		long skipped = processingStep.getSkipCount();

		// The structural-skip rows are NOT in the counters — they leave the read stream
		// before a ProcessingResult exists, so the equation is: read = sum(outcomes).
		if (account != read) {
			throw new BatchInvariantException("Reconciliation failed for businessDate=" + businessDate + ": read="
					+ read + " but classified outcomes=" + account + " (counts=" + counts + ", structuralSkips="
					+ skipped + ")");
		}

		upsertSummary(runId, businessDate, checksum, counts, read, skipped);
		log.info("Reconciliation OK businessDate={} read={} outcomes={} structuralSkips={}", businessDate, read,
				account, skipped);
		return RepeatStatus.FINISHED;
	}

	private StepExecution findProcessingStep(StepExecution stepExecution) {
		return stepExecution.getJobExecution()
			.getStepExecutions()
			.stream()
			.filter(step -> "processCsvStep".equals(step.getStepName()))
			.findFirst()
			.orElseThrow(() -> new BatchInvariantException("processCsvStep execution not found for reconciliation"));
	}

	private Map<String, Long> statusCounts(StepExecution processingStep) {
		Object stored = processingStep.getExecutionContext().get(ClassificationMetricsWriter.KEY_STATUS_COUNTS);
		if (stored == null) {
			return Map.of();
		}
		@SuppressWarnings("unchecked")
		Map<String, Long> counts = (Map<String, Long>) stored;
		return counts;
	}

	private void upsertSummary(String runId, LocalDate businessDate, String checksum, Map<String, Long> counts,
			long read, long skipped) {
		if (this.summaryRepository.findByRunId(runId).isPresent()) {
			// A previous execution already reconciled this run; finalize step owns
			// terminalization. Counts are loaded from the persisted checkpoint, so
			// restart-safe (SPEC 27).
			return;
		}
		java.time.Instant now = java.time.Instant.now();
		long rejected = longAt(counts, "REJECTED_RULE_A") + longAt(counts, "REJECTED_RULE_B");
		this.summaryRepository.save(new BatchRunSummary(null, runId, businessDate, checksum, read,
				longAt(counts, "ACCEPTED"), rejected, longAt(counts, "INVALID"), longAt(counts, "SKIPPED"), now, null,
				null, RunSummaryStatus.RUNNING, now, now));
	}

	private static long longAt(Map<String, Long> counts, String status) {
		Long value = counts.get(status);
		return value == null ? 0L : value;
	}

}
