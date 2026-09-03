package dev.thehenrylabs.springbatchcronjob.observability;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * Structured lifecycle logging (SPEC 46, 47): every important event carries the
 * correlation keys jobName / jobExecutionId / businessDate / fileChecksum / stepName.
 * Per-record logging is prohibited — progress is emitted from
 * {@link ClassificationMetricsWriter}.
 */
@Component
public class BatchLoggingListener implements JobExecutionListener, StepExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(BatchLoggingListener.class);

	@Override
	public void beforeJob(JobExecution jobExecution) {
		intoMDC(jobExecution);
		log.info("Job started jobExecutionId={} name={} businessDate={}", jobExecution.getId(),
				jobExecution.getJobInstance().getJobName(),
				jobExecution.getJobParameters().getString(BatchLaunchService.PARAM_BUSINESS_DATE));
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		log.info("Job finished jobExecutionId={} status={} durationMs={}", jobExecution.getId(),
				jobExecution.getStatus(), durationMs(jobExecution));
		clearMDC();
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		Map<String, String> context = MDC.getCopyOfContextMap();
		if (context != null && !context.isEmpty()) {
			MDC.put("stepName", stepExecution.getStepName());
		}
		log.info("Step started stepName={} jobExecutionId={}", stepExecution.getStepName(),
				stepExecution.getJobExecutionId());
	}

	@Override
	public org.springframework.batch.core.ExitStatus afterStep(StepExecution stepExecution) {
		log.info("Step finished stepName={} jobExecutionId={} status={} read={} write={} skips={}",
				stepExecution.getStepName(), stepExecution.getJobExecutionId(), stepExecution.getStatus(),
				stepExecution.getReadCount(), stepExecution.getWriteCount(), stepExecution.getSkipCount());
		MDC.remove("stepName");
		return stepExecution.getExitStatus();
	}

	private static void intoMDC(JobExecution jobExecution) {
		MDC.put("jobName", jobExecution.getJobInstance().getJobName());
		MDC.put("jobExecutionId", String.valueOf(jobExecution.getId()));
		MDC.put("businessDate", jobExecution.getJobParameters().getString(BatchLaunchService.PARAM_BUSINESS_DATE));
		MDC.put("fileChecksum", jobExecution.getJobParameters().getString(BatchLaunchService.PARAM_FILE_CHECKSUM));
	}

	private static void clearMDC() {
		for (String key : new String[] { "jobName", "jobExecutionId", "businessDate", "fileChecksum", "stepName" }) {
			MDC.remove(key);
		}
	}

	private static long durationMs(JobExecution jobExecution) {
		if (jobExecution.getStartTime() == null || jobExecution.getEndTime() == null) {
			return -1;
		}
		return java.time.Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
	}

	/** Redaction note: no business field values are logged at all (SPEC 46). */

}
