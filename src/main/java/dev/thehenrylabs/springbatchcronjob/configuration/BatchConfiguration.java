package dev.thehenrylabs.springbatchcronjob.configuration;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import dev.thehenrylabs.springbatchcronjob.job.WeeklyBatchJobListener;
import dev.thehenrylabs.springbatchcronjob.job.coordinator.DistributedFinalizeTasklet;
import dev.thehenrylabs.springbatchcronjob.job.coordinator.DistributedProcessingManagerTasklet;
import dev.thehenrylabs.springbatchcronjob.job.coordinator.DistributedReconciliationTasklet;
import dev.thehenrylabs.springbatchcronjob.job.coordinator.IndexAndCreatePartitionsTasklet;
import dev.thehenrylabs.springbatchcronjob.job.reference.ReferenceDataLoader;
import dev.thehenrylabs.springbatchcronjob.job.validation.InputValidationTasklet;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * Coordinator topology. Workers run independently on every pod from durable partition
 * rows.
 */
@Configuration(proxyBeanMethods = false)
public class BatchConfiguration {

	private final JobRepository repository;

	private final PlatformTransactionManager transactions;

	private final InputValidationTasklet validate;

	private final ReferenceDataLoader reference;

	private final IndexAndCreatePartitionsTasklet index;

	private final DistributedProcessingManagerTasklet manager;

	private final DistributedReconciliationTasklet reconcile;

	private final DistributedFinalizeTasklet finalize;

	private final WeeklyBatchJobListener listener;

	public BatchConfiguration(JobRepository repository, PlatformTransactionManager transactions,
			InputValidationTasklet validate, ReferenceDataLoader reference, IndexAndCreatePartitionsTasklet index,
			DistributedProcessingManagerTasklet manager, DistributedReconciliationTasklet reconcile,
			DistributedFinalizeTasklet finalize, WeeklyBatchJobListener listener) {
		this.repository = repository;
		this.transactions = transactions;
		this.validate = validate;
		this.reference = reference;
		this.index = index;
		this.manager = manager;
		this.reconcile = reconcile;
		this.finalize = finalize;
		this.listener = listener;
	}

	@Bean
	Job weeklyCsvCoordinatorJob() {
		return new JobBuilder(BatchLaunchService.JOB_NAME, this.repository)
			.start(step("validateFileStep", this.validate))
			.next(step("loadReferenceDataStep", this.reference))
			.next(step("indexAndCreatePartitionsStep", this.index))
			.next(step("distributedProcessingManagerStep", this.manager))
			.next(step("reconcileStep", this.reconcile))
			.next(step("finalizeStep", this.finalize))
			.listener(this.listener)
			.build();
	}

	private Step step(String name, org.springframework.batch.core.step.tasklet.Tasklet tasklet) {
		return new StepBuilder(name, this.repository).tasklet(tasklet, this.transactions).build();
	}

}
