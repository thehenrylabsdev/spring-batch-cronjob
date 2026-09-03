package dev.thehenrylabs.springbatchcronjob.file;

import java.net.URI;
import java.time.Instant;

/**
 * Resolved input file metadata (SPEC 10). {@code checksum} may be null when the producer
 * cannot provide one; the caller is then responsible for computing it at ingestion time.
 *
 * @param uri canonical source URI
 * @param fileName file name as discovered
 * @param sizeBytes size of the file at discovery time
 * @param lastModified last modification instant
 * @param checksum SHA-256 hex digest, or null if not yet computed
 */
public record ResolvedInputFile(URI uri, String fileName, long sizeBytes, Instant lastModified, String checksum) {

}
