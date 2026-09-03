package dev.thehenrylabs.springbatchcronjob.configuration;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import dev.thehenrylabs.springbatchcronjob.job.WeeklyBatchJobListener;
import dev.thehenrylabs.springbatchcronjob.job.finalize.FinalizeBatchTasklet;
import dev.thehenrylabs.springbatchcronjob.job.processing.ClassificationMetricsWriter;
import dev.thehenrylabs.springbatchcronjob.job.processing.CsvRecordParseException;
import dev.thehenrylabs.springbatchcronjob.job.processing.CsvRecordProcessor;
import dev.thehenrylabs.springbatchcronjob.job.processing.CsvRecordReader;
import dev.thehenrylabs.springbatchcronjob.job.processing.ProcessingStepListener;
import dev.thehenrylabs.springbatchcronjob.job.reconciliation.ReconciliationTasklet;
import dev.thehenrylabs.springbatchcronjob.job.reference.ReferenceDataLoader;
import dev.thehenrylabs.springbatchcronjob.job.validation.InputValidationTasklet;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * weeklyCsvProcessingJob (SPEC 18): validate -> reference -> process -> reconcile ->
 * finalize. Chunk-oriented, fault-tolerant, restart-safe. The job is launched ONLY by
 * BatchLaunchService (spring.batch.job.enabled=false).
 */
@Configuration(proxyBeanMethods = false)
public class BatchConfiguration {

	private final JobRepository jobRepository;

	private final PlatformTransactionManager transactionManager;

	private final BatchProperties properties;

	private final InputValidationTasklet validationTasklet;

	private final ReferenceDataLoader referenceDataLoader;

	private final CsvRecordReader csvRecordReader;

	private final CsvRecordProcessor csvRecordProcessor;

	private final ClassificationMetricsWriter metricsWriter;

	private final ProcessingStepListener processingStepListener;

	private final ReconciliationTasklet reconciliationTasklet;

	private final FinalizeBatchTasklet finalizeBatchTasklet;

	private final WeeklyBatchJobListener jobListener;

	private final ThreadPoolTaskExecutor batchProcessingExecutor;

	public BatchConfiguration(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			BatchProperties properties, InputValidationTasklet validationTasklet,
			ReferenceDataLoader referenceDataLoader, CsvRecordReader csvRecordReader,
			CsvRecordProcessor csvRecordProcessor, ClassificationMetricsWriter metricsWriter,
			ProcessingStepListener processingStepListener, ReconciliationTasklet reconciliationTasklet,
			FinalizeBatchTasklet finalizeBatchTasklet, WeeklyBatchJobListener jobListener,
			ThreadPoolTaskExecutor batchProcessingExecutor) {
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
		this.properties = properties;
		this.validationTasklet = validationTasklet;
		this.referenceDataLoader = referenceDataLoader;
		this.csvRecordReader = csvRecordReader;
		this.csvRecordProcessor = csvRecordProcessor;
		this.metricsWriter = metricsWriter;
		this.processingStepListener = processingStepListener;
		this.reconciliationTasklet = reconciliationTasklet;
		this.finalizeBatchTasklet = finalizeBatchTasklet;
		this.jobListener = jobListener;
		this.batchProcessingExecutor = batchProcessingExecutor;
	}

	@Bean
	Job weeklyCsvProcessingJob() {
		return new JobBuilder(BatchLaunchService.JOB_NAME, this.jobRepository).start(validateInputStep())
			.next(loadReferenceDataStep())
			.next(processCsvStep())
			.next(reconcileStep())
			.next(finalizeStep())
			.listener(this.jobListener)
			.build();
	}

	private org.springframework.batch.core.step.Step validateInputStep() {
		return new StepBuilder("validateInputStep", this.jobRepository)
			.tasklet(this.validationTasklet, this.transactionManager)
			.build();
	}

	private org.springframework.batch.core.step.Step loadReferenceDataStep() {
		return new StepBuilder("loadReferenceDataStep", this.jobRepository)
			.tasklet(this.referenceDataLoader, this.transactionManager)
			.build();
	}

	private org.springframework.batch.core.step.Step processCsvStep() {
		BatchProperties.Processing processing = this.properties.processing();
		BatchProperties.Retry retry = this.properties.retry();

		return new StepBuilder("processCsvStep",
				this.jobRepository).<dev.thehenrylabs.springbatchcronjob.domain.CustomerCsvRecord, dev.thehenrylabs.springbatchcronjob.domain.ProcessingResult>chunk(
						processing.chunkSize())
			.reader(this.csvRecordReader)
			.processor(this.csvRecordProcessor)
			.writer(this.metricsWriter)
			.stream(this.csvRecordReader)
			.stream(this.metricsWriter)
			.listener(this.processingStepListener)
			.taskExecutor(this.batchProcessingExecutor)
			.faultTolerant()
			.skipLimit(processing.skipLimit())
			.skipPolicy(new org.springframework.batch.core.step.skip.LimitCheckingExceptionHierarchySkipPolicy(
					new java.util.LinkedHashSet<>(java.util.List.of(CsvRecordParseException.class)),
					processing.skipLimit()))
			.retryLimit(retry.maxAttempts())
			.retryPolicy(
					new TransientDataAccessRetryPolicy(retry.initialBackoff(), retry.multiplier(), retry.maxBackoff()))
			.build();
	}

	private org.springframework.batch.core.step.Step reconcileStep() {
		return new StepBuilder("reconcileStep", this.jobRepository)
			.tasklet(this.reconciliationTasklet, this.transactionManager)
			.build();
	}

	private org.springframework.batch.core.step.Step finalizeStep() {
		return new StepBuilder("finalizeStep", this.jobRepository)
			.tasklet(this.finalizeBatchTasklet, this.transactionManager)
			.build();
	}

}
