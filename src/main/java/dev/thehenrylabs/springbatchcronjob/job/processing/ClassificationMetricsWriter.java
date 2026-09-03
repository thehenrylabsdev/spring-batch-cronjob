package dev.thehenrylabs.springbatchcronjob.job.processing;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.domain.ProcessingResult;
import dev.thehenrylabs.springbatchcronjob.domain.ProcessingStatus;

/**
 * Lightweight restart-safe sink (SPEC 26, 27): counts results per status, persists a
 * snapshot into the step ExecutionContext at every successful chunk boundary, and
 * restores counts from that snapshot when a failed execution is restarted.
 *
 * <p>
 * No message broker, no per-record persistence, no per-record logging.
 */
@Component
public class ClassificationMetricsWriter implements ItemWriter<ProcessingResult>, ProcessingResultSink,
		org.springframework.batch.infrastructure.item.ItemStream {

	public static final String KEY_STATUS_COUNTS = "classification.statusCounts";

	private static final Logger log = LoggerFactory.getLogger(ClassificationMetricsWriter.class);

	private final Map<ProcessingStatus, LongAdder> counters = new ConcurrentHashMap<>();

	private final dev.thehenrylabs.springbatchcronjob.observability.BatchMetrics metrics;

	private final dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties properties;

	private volatile java.time.Instant stepStartedAt;

	private volatile long lastProgressRecords;

	private volatile long lastProgressAtNanos;

	public ClassificationMetricsWriter(dev.thehenrylabs.springbatchcronjob.observability.BatchMetrics metrics,
			dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties properties) {
		this.metrics = metrics;
		this.properties = properties;
	}

	/**
	 * Satisfies both {@link ItemWriter#write} and {@link ProcessingResultSink#write}: the
	 * counting writer IS the V1 downstream sink (SPEC 26); a broker-backed sink can
	 * decorate this class without touching the reader or processor.
	 */
	@Override
	public void write(Chunk<? extends ProcessingResult> results) {
		long started = System.nanoTime();
		for (ProcessingResult result : results) {
			this.counters.computeIfAbsent(result.status(), ignored -> new LongAdder()).increment();
		}
		this.metrics.writeLatency((System.nanoTime() - started) / Math.max(1, results.size()));
	}

	/**
	 * Checkpoint the counter snapshot at chunk boundaries (SPEC 27). The ItemStream path
	 * is the same mechanism the reader uses — afterChunk-style ChunkListener callbacks do
	 * not get the typed-Chunk dispatch in this Batch build.
	 */
	@Override
	public void update(ExecutionContext executionContext) {
		executionContext.put(KEY_STATUS_COUNTS, snapshot());
		progress();
	}

	/** Marks the step start (from ProcessingStepListener) for progress rate math. */
	public void markStepStarted() {
		this.stepStartedAt = java.time.Instant.now();
		this.lastProgressAtNanos = System.nanoTime();
		this.lastProgressRecords = 0;
	}

	private void progress() {
		long total = total();
		long now = System.nanoTime();
		java.time.Instant started = this.stepStartedAt;
		if (started == null) {
			return;
		}
		long elapsedSeconds = Math.max(1, java.time.Duration.between(started, java.time.Instant.now()).toSeconds());
		long sinceLast = total - this.lastProgressRecords;
		long sinceLastNanos = now - this.lastProgressAtNanos;
		double rate = sinceLast * 1_000_000_000d / Math.max(1L, sinceLastNanos);
		this.metrics.updateRecordsPerSecond(rate);
		boolean byRecords = sinceLast >= this.properties.processing().progressLogRecords();
		boolean byInterval = sinceLastNanos >= this.properties.processing().progressLogIntervalSeconds()
				* 1_000_000_000L;
		if (byRecords || byInterval) {
			log.info("Progress recordsProcessed={} rate={}/s elapsed={}s", total, String.format("%.0f", rate),
					elapsedSeconds);
			this.lastProgressRecords = total;
			this.lastProgressAtNanos = now;
		}
	}

	@Override
	public void open(ExecutionContext executionContext) {
		// counts are restored by ProcessingStepListener.beforeStep
	}

	@Override
	public void close() {
		// nothing to release
	}

	/** Restore counters from the persisted checkpoint (SPEC 27). */
	public void restore(ExecutionContext executionContext) {
		if (executionContext == null) {
			return;
		}
		@SuppressWarnings("unchecked")
		Map<String, Number> stored = (Map<String, Number>) executionContext.get(KEY_STATUS_COUNTS);
		if (stored == null) {
			return;
		}
		for (Map.Entry<String, Number> entry : stored.entrySet()) {
			ProcessingStatus status = ProcessingStatus.valueOf(entry.getKey());
			long value = entry.getValue().longValue();
			if (value > 0) {
				this.counters.computeIfAbsent(status, ignored -> new LongAdder()).add(value);
			}
		}
	}

	public void reset() {
		this.counters.clear();
	}

	public long total() {
		return this.counters.values().stream().mapToLong(LongAdder::sum).sum();
	}

	public long count(ProcessingStatus status) {
		LongAdder adder = this.counters.get(status);
		return adder == null ? 0L : adder.sum();
	}

	/** Snapshot as a persistence-friendly string-keyed map. */
	public Map<String, Long> snapshot() {
		Map<String, Long> snapshot = new java.util.TreeMap<>();
		for (ProcessingStatus status : ProcessingStatus.values()) {
			long value = count(status);
			if (value > 0) {
				snapshot.put(status.name(), value);
			}
		}
		return snapshot;
	}

	public void logSummary() {
		log.info("Classification totals accepted={} rejectedRuleA={} rejectedRuleB={} invalid={} skipped={}",
				count(ProcessingStatus.ACCEPTED), count(ProcessingStatus.REJECTED_RULE_A),
				count(ProcessingStatus.REJECTED_RULE_B), count(ProcessingStatus.INVALID),
				count(ProcessingStatus.SKIPPED));
	}

}
