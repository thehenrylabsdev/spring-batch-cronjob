package dev.thehenrylabs.springbatchcronjob.job.processing;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.support.AbstractItemStreamItemReader;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import dev.thehenrylabs.springbatchcronjob.domain.CustomerCsvRecord;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;

/**
 * Seekable partition reader: it never scans earlier partitions and only emits whole
 * records.
 */
public class ByteRangeCsvItemReader extends AbstractItemStreamItemReader<CustomerCsvRecord> {

	public static final String KEY_NEXT_BYTE = "byteRange.nextByte";

	public static final String KEY_NEXT_RECORD = "byteRange.nextRecord";

	private final Path path;

	private final long endByteExclusive;

	private SeekableByteChannel channel;

	private long nextByte;

	private long nextRecord;

	private CsvParser parser;

	public ByteRangeCsvItemReader(Path path, long checkpointByte, long checkpointRecordNumber, long endByteExclusive) {
		this.path = path;
		this.nextByte = checkpointByte;
		this.nextRecord = checkpointRecordNumber;
		this.endByteExclusive = endByteExclusive;
	}

	@Override
	public void open(ExecutionContext context) {
		this.nextByte = context.getLong(KEY_NEXT_BYTE, this.nextByte);
		this.nextRecord = context.getLong(KEY_NEXT_RECORD, this.nextRecord);
		try {
			this.channel = Files.newByteChannel(this.path);
			this.channel.position(this.nextByte);
		}
		catch (java.io.IOException e) {
			throw new InvalidInputFileException("Cannot seek input " + this.path, e);
		}
		CsvParserSettings settings = new CsvParserSettings();
		settings.setLineSeparatorDetectionEnabled(false);
		this.parser = new CsvParser(settings);
	}

	@Override
	public CustomerCsvRecord read() {
		if (this.nextByte >= this.endByteExclusive)
			return null;
		byte[] raw = nextLogicalRecord();
		if (raw == null)
			return null;
		String[] row;
		try {
			row = this.parser.parseLine(new String(raw, StandardCharsets.UTF_8));
		}
		catch (RuntimeException e) {
			throw new CsvRecordParseException("Malformed CSV record " + (this.nextRecord + 1), e);
		}
		if (row == null || row.length != 5)
			throw new CsvRecordParseException("CSV record has wrong column count at " + (this.nextRecord + 1));
		this.nextRecord++;
		return new CustomerCsvRecord(this.nextRecord, value(row[0]), value(row[1]), value(row[2]), decimal(row[3]),
				date(row[4]));
	}

	@Override
	public void update(ExecutionContext context) {
		context.putLong(KEY_NEXT_BYTE, this.nextByte);
		context.putLong(KEY_NEXT_RECORD, this.nextRecord);
	}

	@Override
	public void close() {
		try {
			if (this.channel != null)
				this.channel.close();
		}
		catch (java.io.IOException ignored) {
		}
	}

	public long nextByteOffset() {
		return this.nextByte;
	}

	public long nextRecordNumber() {
		return this.nextRecord;
	}

	private byte[] nextLogicalRecord() {
		ByteArrayOutputStream record = new ByteArrayOutputStream();
		boolean quoted = false, afterQuote = false;
		ByteBuffer one = ByteBuffer.allocate(1);
		try {
			while (this.nextByte < this.endByteExclusive) {
				one.clear();
				if (this.channel.read(one) != 1)
					throw new InvalidInputFileException("Unexpected EOF inside partition");
				this.nextByte++;
				int c = one.array()[0] & 0xff;
				record.write(c);
				if (quoted) {
					if (c == '"') {
						if (afterQuote) {
							afterQuote = false;
						}
						else {
							afterQuote = true;
						}
					}
					else if (afterQuote) {
						quoted = false;
						afterQuote = false;
					}
				}
				else if (c == '"')
					quoted = true;
				if (c == '\n' && !quoted)
					return trimLineEnding(record.toByteArray());
			}
			if (quoted)
				throw new CsvRecordParseException("Unclosed quoted record at partition end");
			return record.size() == 0 ? null : trimLineEnding(record.toByteArray());
		}
		catch (java.io.IOException e) {
			throw new InvalidInputFileException("Cannot read partition input", e);
		}
	}

	private static byte[] trimLineEnding(byte[] source) {
		int end = source.length;
		if (end > 0 && source[end - 1] == '\n')
			end--;
		if (end > 0 && source[end - 1] == '\r')
			end--;
		return java.util.Arrays.copyOf(source, end);
	}

	private static String value(String value) {
		return value == null || value.isBlank() ? null : value;
	}

	private static BigDecimal decimal(String value) {
		try {
			return value == null || value.isBlank() ? null : new BigDecimal(value.trim());
		}
		catch (NumberFormatException ignored) {
			return null;
		}
	}

	private static LocalDate date(String value) {
		try {
			return value == null || value.isBlank() ? null : LocalDate.parse(value.trim());
		}
		catch (RuntimeException ignored) {
			return null;
		}
	}

}
