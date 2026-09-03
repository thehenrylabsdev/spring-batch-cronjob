package dev.thehenrylabs.springbatchcronjob.file;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.springframework.stereotype.Service;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;

/**
 * Immutable input rule (SPEC 14, strategy 2): after the checksum is known, the discovered
 * file is moved (atomically where possible) into a content-addressed processing location
 * ({@code customers-<sha256>.csv}). The processing job only ever reads this immutable
 * version; re-deliveries resolve to the same destination.
 */
@Service
public class ImmutableInputService {

	private final BatchProperties properties;

	private final FileChecksumService checksumService;

	public ImmutableInputService(BatchProperties properties, FileChecksumService checksumService) {
		this.properties = properties;
		this.checksumService = checksumService;
	}

	/**
	 * Install the file for processing. Returns the (possibly new) immutable location; the
	 * caller must use it for manifest registration and job parameters.
	 *
	 * <p>
	 * If {@code batch.input.processing-location} is blank or identical to the source
	 * directory, the source is returned unchanged (e.g. object storage that is immutable
	 * by nature).
	 */
	public ResolvedInputFile installForProcessing(ResolvedInputFile source, String checksumValue) {
		String processingLocation = this.properties.input().processingLocation();
		if (processingLocation == null || processingLocation.isBlank()) {
			return source;
		}
		URI uri = source.uri();
		if (!"file".equals(uri.getScheme())) {
			// Non-file sources (object storage) are assumed immutable by contract.
			return source;
		}
		Path sourcePath = Path.of(uri);
		Path targetDir = Path.of(processingLocation).toAbsolutePath().normalize();
		if (sourcePath.toAbsolutePath().normalize().getParent().equals(targetDir)) {
			return source;
		}
		Path target = targetDir.resolve("customers-" + checksumValue + "." + extension(sourcePath));
		try {
			Files.createDirectories(targetDir);
			if (Files.exists(target)) {
				// Same content, already installed — idempotent.
				return asResolved(target);
			}
			try {
				Files.move(sourcePath, target, StandardCopyOption.ATOMIC_MOVE);
			}
			catch (java.nio.file.FileSystemException crossDevice) {
				// Covers AtomicMoveNotSupportedException (subtype) and cross-device
				// moves: copy, verify, then drop the source.
				// Different filesystem: copy, verify, then drop the source.
				Files.copy(sourcePath, target, StandardCopyOption.REPLACE_EXISTING);
				String copyChecksum = this.checksumService.sha256(target);
				if (!copyChecksum.equalsIgnoreCase(checksumValue)) {
					Files.deleteIfExists(target);
					throw new InvalidInputFileException(
							"Copied input does not match checksum: " + target + " expected " + checksumValue);
				}
				Files.deleteIfExists(sourcePath);
			}
			return asResolved(target);
		}
		catch (IOException e) {
			throw new InvalidInputFileException(
					"Cannot install immutable input copy for " + sourcePath + " -> " + target, e);
		}
	}

	private static ResolvedInputFile asResolved(Path target) {
		try {
			return new ResolvedInputFile(target.toUri(), target.getFileName().toString(), Files.size(target),
					java.time.Instant.ofEpochMilli(Files.getLastModifiedTime(target).toMillis()), null);
		}
		catch (IOException e) {
			throw new InvalidInputFileException("Cannot stat installed input " + target, e);
		}
	}

	private static String extension(Path path) {
		String name = path.getFileName().toString();
		int dot = name.lastIndexOf('.');
		return dot > 0 ? name.substring(dot + 1) : "csv";
	}

}
