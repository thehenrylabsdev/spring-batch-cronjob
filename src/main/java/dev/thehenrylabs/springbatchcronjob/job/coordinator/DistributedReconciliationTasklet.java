package dev.thehenrylabs.springbatchcronjob.job.coordinator;

import java.time.LocalDate;
import java.util.List;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.domain.BatchPartition;
import dev.thehenrylabs.springbatchcronjob.domain.BatchRunSummary;
import dev.thehenrylabs.springbatchcronjob.domain.PartitionStatus;
import dev.thehenrylabs.springbatchcronjob.domain.RunSummaryStatus;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.repository.BatchRunSummaryRepository;
import dev.thehenrylabs.springbatchcronjob.repository.DistributedPartitionRepository;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;

/** Final proof that every indexed record was covered exactly once by completed ranges. */
@Component
public class DistributedReconciliationTasklet implements Tasklet {

	private final DistributedPartitionRepository partitions;

	private final FileManifestService manifests;

	private final BatchRunSummaryRepository summaries;

	public DistributedReconciliationTasklet(DistributedPartitionRepository partitions, FileManifestService manifests,
			BatchRunSummaryRepository summaries) {
		this.partitions = partitions;
		this.manifests = manifests;
		this.summaries = summaries;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext context) {
		long executionId = context.getStepContext().getStepExecution().getJobExecutionId();
		var params = context.getStepContext().getStepExecution().getJobParameters();
		var manifest = this.manifests
			.findByIdentity(LocalDate.parse(params.getString(BatchLaunchService.PARAM_BUSINESS_DATE)),
					params.getString(BatchLaunchService.PARAM_FILE_CHECKSUM))
			.orElseThrow();
		List<BatchPartition> rows = this.partitions.forManifest(manifest.id());
		if (rows.isEmpty() || rows.stream().anyMatch(p -> p.status() != PartitionStatus.COMPLETED))
			throw new BatchInvariantException("Not all partitions completed");
		long records = 0, bytes = -1, recordCursor = 0;
		for (BatchPartition p : rows) {
			if (bytes >= 0 && (p.startByte() != bytes || p.startRecordNo() != recordCursor))
				throw new BatchInvariantException("Partition ranges contain a gap or overlap");
			if (p.committedProcessedCount() != p.expectedRecordCount())
				throw new BatchInvariantException("Partition result count does not reconcile: " + p.id());
			bytes = p.endByteExclusive();
			recordCursor = p.endRecordNoExclusive();
			records += p.expectedRecordCount();
		}
		if (manifest.fileSizeBytes() != bytes || manifest.expectedRecordCount() == null
				|| manifest.expectedRecordCount() != records)
			throw new BatchInvariantException("Global distributed reconciliation failed");
		long accepted = rows.stream().mapToLong(BatchPartition::acceptedCount).sum();
		long rejected = rows.stream().mapToLong(BatchPartition::rejectedCount).sum();
		long invalid = rows.stream().mapToLong(BatchPartition::invalidCount).sum();
		String runId = Long.toString(executionId);
		if (this.summaries.findByRunId(runId).isEmpty()) {
			var now = java.time.Instant.now();
			this.summaries.save(new BatchRunSummary(null, runId, manifest.businessDate(), manifest.checksumSha256(),
					records, accepted, rejected, invalid, 0, now, now, 0L, RunSummaryStatus.COMPLETED, now, now));
		}
		return RepeatStatus.FINISHED;
	}

}
