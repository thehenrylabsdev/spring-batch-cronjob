package dev.thehenrylabs.springbatchcronjob.job.coordinator;

import java.net.URI;
import java.time.LocalDate;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.domain.BatchFileManifest;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;
import dev.thehenrylabs.springbatchcronjob.service.PartitionPlanningService;

/**
 * Coordinator-only sequential scan; workers cannot claim until its complete coverage
 * proof exists.
 */
@Component
public class IndexAndCreatePartitionsTasklet implements Tasklet {

	private final FileManifestService manifests;

	private final PartitionPlanningService planning;

	public IndexAndCreatePartitionsTasklet(FileManifestService manifests, PartitionPlanningService planning) {
		this.manifests = manifests;
		this.planning = planning;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext context) {
		var parameters = context.getStepContext().getStepExecution().getJobParameters();
		LocalDate date = LocalDate.parse(parameters.getString(BatchLaunchService.PARAM_BUSINESS_DATE));
		String checksum = parameters.getString(BatchLaunchService.PARAM_FILE_CHECKSUM);
		BatchFileManifest manifest = this.manifests.findByIdentity(date, checksum)
			.orElseThrow(() -> new BatchInvariantException("Manifest disappeared before indexing"));
		if (manifest.status() == dev.thehenrylabs.springbatchcronjob.domain.ManifestStatus.READY)
			return RepeatStatus.FINISHED; // coordinator restart: keep the original safe
											// ranges.
		if (manifest.status() != dev.thehenrylabs.springbatchcronjob.domain.ManifestStatus.INDEXING)
			throw new BatchInvariantException("Manifest is not ready to index: " + manifest.status());
		var index = this.planning.indexAndPersist(manifest.id(),
				context.getStepContext().getStepExecution().getJobExecutionId(), URI.create(manifest.fileUri()));
		this.manifests.finishIndex(manifest.id(), index.expectedRecordCount());
		return RepeatStatus.FINISHED;
	}

}
