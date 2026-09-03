package dev.thehenrylabs.springbatchcronjob.job.processing;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import org.springframework.stereotype.Component;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.domain.CustomerCsvRecord;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.util.StringUtils;

/**
 * Streaming single-pass CSV reader (SPEC 7, 22):
 *
 * <ul>
 * <li>logical CSV records (quoted, multiline-safe) through univocity;
 * <li>restart anchor: number of already-committed logical records; on resume the header
 * plus that many records are skipped (parse-and-discard — no byte-offset arithmetic that
 * could fragment multiline records, ADR-0005/0007);
 * <li>structural parse failures become {@link CsvRecordParseException} (skip policy);
 * field-level parse problems (amount/date) surface as null fields -> INVALID.
 * </ul>
 */
@Component
public class CsvRecordReader implements ItemReader<CustomerCsvRecord>, ItemStream {

	/**
	 * Step execution context key for the file URI to read (set by the pre-step listener).
	 */
	public static final String KEY_FILE_URI = "csvReader.fileUri";

	public static final String KEY_RECORD_NUMBER = "csvReader.recordNumber";

	private final BatchProperties properties;

	private final RecordBoundaryFailureInjector failureInjector;

	private CsvParser parser;

	private InputStream input;

	private String[] header;

	private long emittedRecords;

	public CsvRecordReader(BatchProperties properties,
			org.springframework.beans.factory.ObjectProvider<RecordBoundaryFailureInjector> failureInjector) {
		this.properties = properties;
		this.failureInjector = failureInjector.getIfAvailable(RecordBoundaryFailureInjector.Noop::new);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		String fileUri = executionContext.getString(KEY_FILE_URI);
		if (!StringUtils.hasText(fileUri)) {
			throw new InvalidInputFileException("No fileUri in step execution context — pre-step listener missing");
		}
		Path file = Path.of(java.net.URI.create(fileUri));
		initParser();
		try {
			this.input = Files.newInputStream(file);
		}
		catch (java.io.IOException e) {
			throw new InvalidInputFileException("Cannot open input " + file, e);
		}
		this.parser.beginParsing(
				new java.io.InputStreamReader(this.input, Charset.forName(this.properties.input().expectedEncoding())));
		this.header = this.parser.parseNext();
		if (this.header == null) {
			throw new InvalidInputFileException("CSV has no header row: " + file);
		}

		long resumeAt = executionContext.getLong(KEY_RECORD_NUMBER, 0L);
		long skipped = 0;
		while (skipped < resumeAt) {
			if (this.parser.parseNext() == null) {
				throw new InvalidInputFileException(
						"Restart recorded " + resumeAt + " committed records beyond end of file: " + file);
			}
			skipped++;
		}
		this.emittedRecords = resumeAt;
	}

	private void initParser() {
		CsvParserSettings settings = new CsvParserSettings();
		settings.getFormat().setDelimiter(this.properties.input().expectedDelimiter().charAt(0));
		settings.setMaxCharsPerColumn(1024 * 1024);
		this.parser = new CsvParser(settings);
	}

	@Override
	public void update(ExecutionContext executionContext) {
		executionContext.putLong(KEY_RECORD_NUMBER, this.emittedRecords);
	}

	@Override
	public void close() {
		if (this.parser != null) {
			this.parser.stopParsing();
		}
		if (this.input != null) {
			try {
				this.input.close();
			}
			catch (java.io.IOException ignored) {
				// best effort close
			}
		}
	}

	@Override
	public CustomerCsvRecord read() {
		String[] row;
		try {
			row = this.parser.parseNext();
		}
		catch (RuntimeException e) {
			throw new CsvRecordParseException("Structural CSV parse failure at record " + (this.emittedRecords + 1), e);
		}
		if (row == null) {
			return null;
		}
		this.emittedRecords++;
		this.failureInjector.maybeThrow(this.emittedRecords);
		if (row.length != this.header.length) {
			throw new CsvRecordParseException("CSV record " + this.emittedRecords + " has " + row.length
					+ " columns, expected " + this.header.length);
		}
		long recordNumber = this.emittedRecords;
		return new CustomerCsvRecord(recordNumber, value(row, 0), value(row, 1), value(row, 2), decimal(row, 3),
				date(row, 4));
	}

	private static String value(String[] row, int index) {
		String v = row[index];
		return v == null || v.isBlank() ? null : v;
	}

	private static BigDecimal decimal(String[] row, int index) {
		String v = row[index];
		if (v == null || v.isBlank()) {
			return null;
		}
		try {
			return new BigDecimal(v.trim());
		}
		catch (NumberFormatException e) {
			return null; // classified INVALID by the processor
		}
	}

	private static LocalDate date(String[] row, int index) {
		String v = row[index];
		if (v == null || v.isBlank()) {
			return null;
		}
		try {
			return LocalDate.parse(v.trim());
		}
		catch (RuntimeException e) {
			return null; // classified INVALID by the processor
		}
	}

}
