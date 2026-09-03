package dev.thehenrylabs.springbatchcronjob.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Compact immutable input record (SPEC 23). Field-level parse failures (amount, date) are
 * represented as {@code null} and classified as INVALID by the processor.
 *
 * @param logicalRecordNumber 1-based data record position in the file (restart anchor)
 * @param customerId customer identifier
 * @param fieldA generic string field
 * @param fieldB generic string field
 * @param amount decimal amount, nullable when malformed
 * @param eventDate event date, nullable when malformed
 */
public record CustomerCsvRecord(long logicalRecordNumber, String customerId, String fieldA, String fieldB,
		BigDecimal amount, LocalDate eventDate) {

}
