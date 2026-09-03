package dev.thehenrylabs.springbatchcronjob.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * Representative CSV generator for scale tests (SPEC 61.5, 56): realistic mixes of quoted
 * fields, commas inside quotes, unicode, boundary dates, and a configurable rate of
 * malformed (INVALID) and suppressed (SKIPPED) rows, so the classification distribution
 * is non-trivial.
 */
public final class CsvDataGenerator {

	private CsvDataGenerator() {
	}

	/** Write {@code records} data rows and return the file. */
	public static Path write(Path dir, String fileName, int records, int seed) throws IOException {
		Random random = new Random(seed);
		Path file = dir.resolve(fileName);
		try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
			writer.write("customer_id,field_a,field_b,amount,event_date\n");
			String[] lastNames = { "Nguyen", "Trần", "Lé", "Dương", "Vũ", "Hoàng", "Smith", "Müller", "田中", "Ива́нов" };
			for (int i = 1; i <= records; i++) {
				String id = "U" + String.format("%03d", (i % 500) + 1);
				String fieldA = i % 97 == 0 ? "S" : "a" + (i % 5);
				String fieldB;
				switch (i % 7) {
					case 0 -> fieldB = "\"" + lastNames[random.nextInt(lastNames.length)] + ", party\""; // quoted
																											// comma
					case 1 -> fieldB = lastNames[random.nextInt(lastNames.length)] + " " + (1000 + i % 9000);
					case 2 -> fieldB = "\"multi\nline " + i % 100 + "\""; // multiline
																			// quoted
																			// field
					default -> fieldB = "value-" + i % 89;
				}
				String amount = i % 11 == 0 ? "not-a-number" : String.format("%.2f", 10 + random.nextDouble() * 990);
				String date = switch (i % 13) {
					case 0 -> "2020-01-01"; // out of 365-day window
					case 1 -> "2099-12-31";
					default -> "2026-08-31";
				};
				writer.write(id);
				writer.write(',');
				writer.write(fieldA);
				writer.write(',');
				writer.write(fieldB);
				writer.write(',');
				writer.write(amount);
				writer.write(',');
				writer.write(date);
				writer.write('\n');
			}
		}
		return file;
	}

}
