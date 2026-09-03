package dev.thehenrylabs.springbatchcronjob.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.springframework.stereotype.Service;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;

/**
 * Local filesystem resolver restricted to {@code batch.input.location} (SPEC 10, 50). The
 * file is located by naming convention (<code>customers</code> + separator +
 * <code>yyyyMMdd</code> + filename tail); the best candidate is the newest file that
 * matches the naming pattern, so a re-delivered file wins by content-identity checksum
 * downstream.
 */
@Service
public class LocalInputFileResolver implements InputFileResolver {

	private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

	private final BatchProperties properties;

	public LocalInputFileResolver(BatchProperties properties) {
		this.properties = properties;
	}

	@Override
	public ResolvedInputFile resolve(LocalDate businessDate) {
		Path base = Path.of(this.properties.input().location()).toAbsolutePath().normalize();
		Pattern pattern = Pattern.compile(this.properties.input().fileNamePattern());
		String dateToken = businessDate.format(DATE_FORMAT);

		List<Path> candidates;
		try {
			try (var stream = Files.list(base)) {
				candidates = stream.filter(Files::isRegularFile)
					.filter(path -> path.getFileName().toString().contains(dateToken))
					.filter(path -> pattern.matcher(path.getFileName().toString()).matches())
					.map(path -> path.toAbsolutePath().normalize())
					.filter(path -> path.startsWith(base)) // traversal defense (SPEC 50)
					.toList();
			}
		}
		catch (IOException e) {
			throw new InvalidInputFileException("Cannot list input location " + base, e);
		}

		if (candidates.isEmpty()) {
			throw new InvalidInputFileException("No input file matching '" + this.properties.input().fileNamePattern()
					+ "' for business date " + businessDate + " in " + base);
		}

		Path best = candidates.stream()
			.max(Comparator.comparingLong((Path p) -> lastModified(p)).thenComparing(Path::toString))
			.orElseThrow();

		if (!Files.isReadable(best)) {
			throw new InvalidInputFileException("Input file is not readable: " + best);
		}
		long size;
		try {
			size = Files.size(best);
		}
		catch (IOException e) {
			throw new InvalidInputFileException("Cannot stat input file " + best, e);
		}
		if (size == 0) {
			throw new InvalidInputFileException("Input file is empty: " + best);
		}
		return new ResolvedInputFile(best.toUri(), best.getFileName().toString(), size,
				java.time.Instant.ofEpochMilli(lastModified(best)), null);
	}

	private static long lastModified(Path path) {
		try {
			return Files.getLastModifiedTime(path).toMillis();
		}
		catch (IOException e) {
			return Long.MIN_VALUE;
		}
	}

}
