package dev.thehenrylabs.springbatchcronjob.domain;

/**
 * Classification output for exactly one input record (SPEC 24).
 *
 * @param logicalRecordNumber record position, for diagnostics and reconciliation
 * @param businessKey identity used for downstream dedup (e.g. customer id)
 * @param status classification outcome
 * @param reasonCode deterministic reason (null/"" for ACCEPTED)
 */
public record ProcessingResult(long logicalRecordNumber, String businessKey, ProcessingStatus status,
		String reasonCode) {

}
