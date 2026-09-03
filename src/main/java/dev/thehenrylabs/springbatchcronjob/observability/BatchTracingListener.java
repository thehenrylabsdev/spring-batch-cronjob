package dev.thehenrylabs.springbatchcronjob.observability;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.stereotype.Component;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * Tracing spans at job granularity (SPEC 48): Quartz trigger -> job -> step. No span is
 * created per record — chunk-level tracing is intentionally omitted in v1 (10M spans per
 * job are operationally expensive). Wiring the OTel bridge + exporter makes these
 * Observations exportable; without a bridge the registry collects nothing.
 */
@Component
public class BatchTracingListener implements JobExecutionListener {

	private final ObservationRegistry registry;

	private final Map<Long, Observation> active = new ConcurrentHashMap<>();

	public BatchTracingListener(ObservationRegistry registry) {
		this.registry = registry;
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {
		Observation observation = Observation
			.createNotStarted("weekly-csv-job", Observation.Context::new, this.registry)
			.lowCardinalityKeyValue("jobName", jobExecution.getJobInstance().getJobName())
			.lowCardinalityKeyValue("jobExecutionId", String.valueOf(jobExecution.getId()));
		observation.start();
		this.active.put(jobExecution.getId(), observation);
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		Observation observation = this.active.remove(jobExecution.getId());
		if (observation != null) {
			observation.lowCardinalityKeyValue("status", jobExecution.getStatus().name());
			observation.stop();
		}
	}

}
