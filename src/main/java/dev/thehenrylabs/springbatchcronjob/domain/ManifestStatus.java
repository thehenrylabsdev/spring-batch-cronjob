package dev.thehenrylabs.springbatchcronjob.domain;

/**
 * Lifecycle of an input file in {@code control.batch_file_manifest} (SPEC 12).
 *
 * <pre>
 * DISCOVERED -> VALIDATING -> READY -> PROCESSING -> COMPLETED
 *                                   |             |-> FAILED
 *                                   |             |-> QUARANTINED
 *                                   |-> FAILED
 * </pre>
 */
public enum ManifestStatus {

	DISCOVERED, VALIDATING, READY, PROCESSING, COMPLETED, FAILED, QUARANTINED

}
