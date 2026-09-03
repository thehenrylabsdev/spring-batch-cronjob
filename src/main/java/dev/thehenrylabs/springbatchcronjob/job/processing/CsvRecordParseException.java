package dev.thehenrylabs.springbatchcronjob.job.processing;

/**
 * Structural CSV parse failure (SPEC 34.2): the record does not honor the CSV contract
 * (unbalanced quote, etc.). These are SKIPPED under the configured skip limit — they are
 * deterministic and never retried (SPEC 35).
 */
public class CsvRecordParseException extends RuntimeException {

	public CsvRecordParseException(String message) {
		super(message);
	}

	public CsvRecordParseException(String message, Throwable cause) {
		super(message, cause);
	}

}
