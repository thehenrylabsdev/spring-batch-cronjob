package dev.thehenrylabs.springbatchcronjob.job.processing;

import java.math.BigDecimal;

import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.domain.CustomerCsvRecord;
import dev.thehenrylabs.springbatchcronjob.domain.ProcessingResult;
import dev.thehenrylabs.springbatchcronjob.domain.ProcessingStatus;
import dev.thehenrylabs.springbatchcronjob.domain.RecordClassifier;

/**
 * Deterministic, stateless, thread-safe classification (SPEC 24, 32). Business rules are
 * evaluated exclusively against the {@link ProcessingContext} snapshot and the record —
 * no time-of-day dependence, no external calls.
 */
@Component
public class CustomerRecordClassifier implements RecordClassifier<CustomerCsvRecord, ProcessingResult> {

	static final BigDecimal AMOUNT_CAP = new BigDecimal("1000000");

	static final int MAX_EVENT_AGE_DAYS = 365;

	static final int MAX_EVENT_FUTURE_DAYS = 30;

	private final ProcessingContext processingContext;

	public CustomerRecordClassifier(ProcessingContext processingContext) {
		this.processingContext = processingContext;
	}

	@Override
	public ProcessingResult classify(CustomerCsvRecord record) {
		ProcessingContext.Snapshot context = this.processingContext.current();
		String businessKey = record.customerId();

		if (record.customerId() == null) {
			return result(record, businessKey, ProcessingStatus.INVALID, "REQUIRED_CUSTOMER_ID");
		}
		if (record.amount() == null) {
			return result(record, businessKey, ProcessingStatus.INVALID, "REQUIRED_AMOUNT");
		}
		if (record.amount().signum() < 0) {
			return result(record, businessKey, ProcessingStatus.INVALID, "AMOUNT_NEGATIVE");
		}
		if (record.amount().compareTo(AMOUNT_CAP) > 0) {
			return result(record, businessKey, ProcessingStatus.REJECTED_RULE_A, "AMOUNT_LIMIT_EXCEEDED");
		}
		if (record.eventDate() == null) {
			return result(record, businessKey, ProcessingStatus.INVALID, "REQUIRED_EVENT_DATE");
		}
		if (record.eventDate().isBefore(context.businessDate().minusDays(MAX_EVENT_AGE_DAYS))
				|| record.eventDate().isAfter(context.businessDate().plusDays(MAX_EVENT_FUTURE_DAYS))) {
			return result(record, businessKey, ProcessingStatus.REJECTED_RULE_B, "EVENT_DATE_OUT_OF_RANGE");
		}
		if ("S".equals(record.fieldA())) {
			return result(record, businessKey, ProcessingStatus.SKIPPED, "SUPPRESSED");
		}
		if (!context.eligibleCustomerIds().contains(record.customerId())) {
			return result(record, businessKey, ProcessingStatus.REJECTED_RULE_B, "NOT_ELIGIBLE");
		}
		return result(record, businessKey, ProcessingStatus.ACCEPTED, "OK");
	}

	private static ProcessingResult result(CustomerCsvRecord record, String businessKey, ProcessingStatus status,
			String reason) {
		return new ProcessingResult(record.logicalRecordNumber(), businessKey, status, reason);
	}

}
