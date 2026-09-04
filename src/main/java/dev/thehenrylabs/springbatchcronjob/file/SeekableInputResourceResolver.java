package dev.thehenrylabs.springbatchcronjob.file;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;

import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;

/**
 * Resolves only seekable file/classpath resources; classpath bytes are copied per pod.
 */
@Component
public class SeekableInputResourceResolver {

	private final ResourceLoader resourceLoader;

	private final BatchProperties properties;

	public SeekableInputResourceResolver(ResourceLoader resourceLoader, BatchProperties properties) {
		this.resourceLoader = resourceLoader;
		this.properties = properties;
	}

	public ResolvedInputFile resolve() {
		String location = this.properties.input().resource();
		if (location == null || location.isBlank()) {
			throw new InvalidInputFileException("batch.input.resource must be configured");
		}
		if (location.startsWith("file:")) {
			return file(Path.of(URI.create(location)));
		}
		if (!location.startsWith("classpath:")) {
			throw new InvalidInputFileException("Only file: and classpath: input resources are supported: " + location);
		}
		return materialize(location);
	}

	private ResolvedInputFile file(Path path) {
		try {
			Path normalized = path.toAbsolutePath().normalize();
			if (!Files.isRegularFile(normalized) || !Files.isReadable(normalized)) {
				throw new InvalidInputFileException("Input is not a readable regular file: " + normalized);
			}
			return new ResolvedInputFile(normalized.toUri(), normalized.getFileName().toString(),
					Files.size(normalized), Files.getLastModifiedTime(normalized).toInstant(), null);
		}
		catch (IOException e) {
			throw new InvalidInputFileException("Cannot stat input " + path, e);
		}
	}

	private ResolvedInputFile materialize(String location) {
		Resource resource = this.resourceLoader.getResource(location);
		if (!resource.exists()) {
			throw new InvalidInputFileException("Classpath input does not exist: " + location);
		}
		String leaf = location.substring(location.lastIndexOf('/') + 1);
		Path destination = Path.of(System.getProperty("java.io.tmpdir"), "cronjob-batch", "classpath", leaf)
			.toAbsolutePath()
			.normalize();
		try {
			Files.createDirectories(destination.getParent());
			try (InputStream source = resource.getInputStream()) {
				Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
			}
			Files.setLastModifiedTime(destination, java.nio.file.attribute.FileTime.from(Instant.EPOCH));
			return new ResolvedInputFile(destination.toUri(), leaf, Files.size(destination), Instant.EPOCH, null);
		}
		catch (IOException e) {
			throw new InvalidInputFileException("Cannot materialize classpath input " + location, e);
		}
	}

}
