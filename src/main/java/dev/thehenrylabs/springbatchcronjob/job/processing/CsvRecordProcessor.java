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

	public CsvRecordProcessor(CustomerRecordClassifier classifier) {
		this.classifier = classifier;
	}

	@Override
	public ProcessingResult process(CustomerCsvRecord record) {
		return this.classifier.classify(record);
	}

}
