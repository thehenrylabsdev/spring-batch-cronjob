package dev.thehenrylabs.springbatchcronjob.job.coordinator;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.configuration.DistributedBatchProperties;
import dev.thehenrylabs.springbatchcronjob.domain.BatchPartition;
import dev.thehenrylabs.springbatchcronjob.domain.PartitionStatus;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.repository.DistributedPartitionRepository;
import dev.thehenrylabs.springbatchcronjob.repository.DistributedWorkerRepository;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;
import dev.thehenrylabs.springbatchcronjob.service.PartitionLeaseService;

/** Waits on durable state only; it never assigns a partition to a particular pod. */
@Component
public class DistributedProcessingManagerTasklet implements Tasklet {

	private final FileManifestService manifests;

	private final DistributedPartitionRepository partitions;

	private final DistributedWorkerRepository workers;

	private final PartitionLeaseService leases;

	private final DistributedBatchProperties properties;

	private final Clock clock;

	public DistributedProcessingManagerTasklet(FileManifestService manifests, DistributedPartitionRepository partitions,
			DistributedWorkerRepository workers, PartitionLeaseService leases, DistributedBatchProperties properties,
			Clock clock) {
		this.manifests = manifests;
		this.partitions = partitions;
		this.workers = workers;
		this.leases = leases;
		this.properties = properties;
		this.clock = clock;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext context) throws Exception {
		var parameters = context.getStepContext().getStepExecution().getJobParameters();
		var manifest = this.manifests
			.findByIdentity(LocalDate.parse(parameters.getString(BatchLaunchService.PARAM_BUSINESS_DATE)),
					parameters.getString(BatchLaunchService.PARAM_FILE_CHECKSUM))
			.orElseThrow();
		if (manifest.status() == dev.thehenrylabs.springbatchcronjob.domain.ManifestStatus.READY)
			this.manifests.transitionToProcessing(manifest.id());
		Instant withoutWorkersSince = null;
		long executionId = context.getStepContext().getStepExecution().getJobExecutionId();
		while (true) {
			this.leases.reapExpired();
			List<BatchPartition> rows = this.partitions.forExecution(executionId);
			if (rows.isEmpty())
				throw new BatchInvariantException("No partitions persisted for coordinator execution " + executionId);
			if (rows.stream().allMatch(row -> row.status() == PartitionStatus.COMPLETED))
				return RepeatStatus.FINISHED;
			if (rows.stream()
				.anyMatch(row -> row.status() == PartitionStatus.FAILED
						|| row.attemptCount() >= this.properties.partition().maxAttempts()))
				throw new BatchInvariantException("A distributed partition exhausted its retry budget");
			int active = this.workers.activeSlots(this.clock.instant().minus(this.properties.worker().activeTimeout()));
			if (active == 0) {
				if (withoutWorkersSince == null)
					withoutWorkersSince = this.clock.instant();
				if (withoutWorkersSince.plus(this.properties.noWorkerTimeout()).isBefore(this.clock.instant()))
					throw new BatchInvariantException("No active distributed workers before timeout");
			}
			else
				withoutWorkersSince = null;
			Thread.sleep(Math.min(1000, this.properties.worker().idlePollInterval().toMillis()));
		}
	}

}
