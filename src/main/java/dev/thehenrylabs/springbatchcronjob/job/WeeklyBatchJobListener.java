package dev.thehenrylabs.springbatchcronjob.job;

import java.time.LocalDate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;

/**
 * Terminal job listener: a FAILED job MUST leave the manifest in FAILED (SPEC 38, 41)
 * while the immutable input and all checkpoint state stay intact for restart. Idempotent:
 * transitions already terminal are left untouched.
 */
@Component
public class WeeklyBatchJobListener implements JobExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(WeeklyBatchJobListener.class);

	private final FileManifestService manifestService;

	private final dev.thehenrylabs.springbatchcronjob.observability.BatchMetrics metrics;

	public WeeklyBatchJobListener(FileManifestService manifestService,
			dev.thehenrylabs.springbatchcronjob.observability.BatchMetrics metrics) {
		this.manifestService = manifestService;
		this.metrics = metrics;
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {
		this.metrics.jobStarted();
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.FAILED) {
			this.metrics.jobFailed();
		}
		else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			this.metrics.jobCompleted();
		}
		if (jobExecution.getStartTime() != null && jobExecution.getEndTime() != null) {
			this.metrics.jobDurationNanos(
					java.time.Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toNanos());
		}
		if (jobExecution.getStatus() != BatchStatus.FAILED) {
			return;
		}
		String businessDate = jobExecution.getJobParameters().getString(BatchLaunchService.PARAM_BUSINESS_DATE);
		String checksum = jobExecution.getJobParameters().getString(BatchLaunchService.PARAM_FILE_CHECKSUM);
		this.manifestService.findByIdentity(LocalDate.parse(businessDate), checksum).ifPresent(manifest -> {
			if (manifest.status() != dev.thehenrylabs.springbatchcronjob.domain.ManifestStatus.FAILED) {
				try {
					this.manifestService.transitionToFailed(manifest.id());
					log.warn("Job failed businessDate={} checksum={} manifestId={} — marked FAILED", businessDate,
							checksum, manifest.id());
				}
				catch (RuntimeException unchanged) {
					// e.g. already QUARANTINED/COMPLETED: keep the existing terminal
					// state
					log.debug("Manifest {} could not be marked FAILED: {}", manifest.id(), unchanged.getMessage());
				}
			}
		});
	}

}
