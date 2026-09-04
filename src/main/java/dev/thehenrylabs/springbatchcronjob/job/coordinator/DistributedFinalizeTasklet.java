package dev.thehenrylabs.springbatchcronjob.job.coordinator;

import java.time.LocalDate;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;

/** Moves the manifest terminal only after manager and reconciliation have succeeded. */
@Component
public class DistributedFinalizeTasklet implements Tasklet {

	private final FileManifestService manifests;

	public DistributedFinalizeTasklet(FileManifestService manifests) {
		this.manifests = manifests;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext context) {
		var params = context.getStepContext().getStepExecution().getJobParameters();
		var manifest = this.manifests
			.findByIdentity(LocalDate.parse(params.getString(BatchLaunchService.PARAM_BUSINESS_DATE)),
					params.getString(BatchLaunchService.PARAM_FILE_CHECKSUM))
			.orElseThrow();
		this.manifests.transitionToCompleted(manifest.id(), manifest.expectedRecordCount());
		return RepeatStatus.FINISHED;
	}

}
