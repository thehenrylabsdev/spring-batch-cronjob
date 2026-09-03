package dev.thehenrylabs.springbatchcronjob.job.processing;

import org.springframework.stereotype.Component;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import dev.thehenrylabs.springbatchcronjob.domain.CustomerCsvRecord;
import dev.thehenrylabs.springbatchcronjob.domain.ProcessingResult;

/**
 * ItemProcessor adapter over the deterministic classifier (SPEC 24). Stateless and
 * thread-safe; never performs I/O.
 */
@Component
public class CsvRecordProcessor implements ItemProcessor<CustomerCsvRecord, ProcessingResult> {

	private final CustomerRecordClassifier classifier;

	private final dev.thehenrylabs.springbatchcronjob.observability.BatchMetrics metrics;

	public CsvRecordProcessor(CustomerRecordClassifier classifier,
			dev.thehenrylabs.springbatchcronjob.observability.BatchMetrics metrics) {
		this.classifier = classifier;
		this.metrics = metrics;
	}

	@Override
	public ProcessingResult process(CustomerCsvRecord record) {
		long started = System.nanoTime();
		ProcessingResult result = this.classifier.classify(record);
		this.metrics.processLatency(System.nanoTime() - started);
		this.metrics.recordProcessed();
		this.metrics.recordResult(result.status());
		return result;
	}

}
