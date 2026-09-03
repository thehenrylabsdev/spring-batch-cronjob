package dev.thehenrylabs.springbatchcronjob.observability;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.DoubleAdder;

import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.domain.ProcessingStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * All Micrometer batch metrics (SPEC 45). Names are stable contract: dashboards and
 * alerting reference them.
 */
@Component
public class BatchMetrics {

	private final MeterRegistry registry;

	private final DoubleAdder latestRecordsPerSecond = new DoubleAdder();

	public BatchMetrics(MeterRegistry registry) {
		this.registry = registry;
		Gauge.builder("batch_records_per_second", this.latestRecordsPerSecond, DoubleAdder::sum).register(registry);
	}

	// Job metrics

	public void jobStarted() {
		counter("batch_job_started_total").increment();
	}

	public void jobCompleted() {
		counter("batch_job_completed_total").increment();
	}

	public void jobFailed() {
		counter("batch_job_failed_total").increment();
	}

	public void jobDuration(long seconds) {
		Timer.builder("batch_job_duration_seconds").register(this.registry).record(seconds, TimeUnit.SECONDS);
	}

	public void jobDurationNanos(long durationNanos) {
		Timer.builder("batch_job_duration_seconds").register(this.registry).record(durationNanos, TimeUnit.NANOSECONDS);
	}

	// Record metrics

	public void recordRead() {
		counter("batch_records_read_total").increment();
	}

	public void recordProcessed() {
		counter("batch_records_processed_total").increment();
	}

	public void recordResult(ProcessingStatus status) {
		counter(statusMetric(status)).increment();
	}

	// Performance metrics

	public void chunkDuration(long nanos) {
		Timer.builder("batch_chunk_duration_seconds").register(this.registry).record(nanos, TimeUnit.NANOSECONDS);
	}

	public void readLatency(long nanos) {
		Timer.builder("batch_reader_latency_seconds").register(this.registry).record(nanos, TimeUnit.NANOSECONDS);
	}

	public void processLatency(long nanos) {
		Timer.builder("batch_processor_latency_seconds").register(this.registry).record(nanos, TimeUnit.NANOSECONDS);
	}

	public void writeLatency(long nanos) {
		Timer.builder("batch_writer_latency_seconds").register(this.registry).record(nanos, TimeUnit.NANOSECONDS);
	}

	public void updateRecordsPerSecond(double rate) {
		this.latestRecordsPerSecond.reset();
		this.latestRecordsPerSecond.add(rate);
	}

	// Scheduler metrics

	public void quartzTriggerFired() {
		counter("quartz_trigger_fired_total").increment();
	}

	public void quartzTriggerMisfire() {
		counter("quartz_trigger_misfire_total").increment();
	}

	private Counter counter(String name) {
		return this.registry.counter(name);
	}

	private static String statusMetric(ProcessingStatus status) {
		return switch (status) {
			case ACCEPTED -> "batch_records_accepted_total";
			case REJECTED_RULE_A, REJECTED_RULE_B -> "batch_records_rejected_total";
			case INVALID -> "batch_records_invalid_total";
			case SKIPPED -> "batch_records_skipped_total";
		};
	}

}
