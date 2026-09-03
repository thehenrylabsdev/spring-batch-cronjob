package dev.thehenrylabs.springbatchcronjob.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.springframework.stereotype.Component;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;

/**
 * Header / schema contract validation (SPEC 13). Only a bounded prefix of the file is
 * read — the 10M-record body is never scanned here.
 */
@Component
public class CsvSchemaValidator {

	private static final int PROBE_ROWS = 10;

	private static final int ENCODING_PROBE_BYTES = 64 * 1024;

	/**
	 * Validates encoding, delimiter-derived shape and the expected header of the input.
	 * @throws InvalidInputFileException on any contract violation
	 */
	public void validate(Path file, BatchProperties.Input input) {
		Charset charset = Charset.forName(input.expectedEncoding());
		verifyEncoding(file, charset);
		CsvParserSettings settings = new CsvParserSettings();
		settings.getFormat().setDelimiter(input.expectedDelimiter().charAt(0));
		settings.setMaxCharsPerColumn(1024 * 1024);
		CsvParser parser = new CsvParser(settings);
		try {
			parser.beginParsing(new InputStreamReader(Files.newInputStream(file), charset));
			String[] header = parser.parseNext();
			if (header == null) {
				throw new InvalidInputFileException("CSV has no header row: " + file);
			}
			List<String> actual = List.of(header);
			if (!actual.equals(input.expectedHeader())) {
				throw new InvalidInputFileException(
						"CSV header mismatch: expected " + input.expectedHeader() + " but was " + actual);
			}
			// Probe a few rows so structural errors (broken quoting, too few
			// columns) are caught before the main processing phase.
			int probes = 0;
			while (probes++ < PROBE_ROWS) {
				String[] row = parser.parseNext();
				if (row == null) {
					break;
				}
				if (row.length != header.length) {
					throw new InvalidInputFileException("CSV row has " + row.length + " columns, expected "
							+ header.length + " near start of " + file);
				}
			}
		}
		catch (IOException e) {
			throw new InvalidInputFileException("Cannot read input for schema validation: " + file, e);
		}
		finally {
			parser.stopParsing();
		}
	}

	private void verifyEncoding(Path file, Charset expected) {
		byte[] prefix;
		try {
			prefix = readPrefix(file);
		}
		catch (IOException e) {
			throw new InvalidInputFileException("Cannot read input for encoding validation: " + file, e);
		}
		CharsetDecoder decoder = expected.newDecoder()
			.onMalformedInput(CodingErrorAction.REPORT)
			.onUnmappableCharacter(CodingErrorAction.REPORT);
		try {
			// Drop the trailing bytes: a multibyte character may be split at the
			// probe boundary, which must not be treated as a malformed sequence.
			int head = Math.max(0, prefix.length - 4);
			decoder.decode(ByteBuffer.wrap(prefix, 0, head));
		}
		catch (CharacterCodingException e) {
			throw new InvalidInputFileException("Input is not valid " + expected.name() + ": " + file, e);
		}
	}

	private static byte[] readPrefix(Path file) throws IOException {
		try (InputStream in = Files.newInputStream(file)) {
			return in.readNBytes(ENCODING_PROBE_BYTES);
		}
	}

}
