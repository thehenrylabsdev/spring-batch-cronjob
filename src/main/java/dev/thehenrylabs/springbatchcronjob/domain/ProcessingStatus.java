package dev.thehenrylabs.springbatchcronjob.domain;

/**
 * Deterministic classification outcomes (SPEC 24). Business rejections are NOT
 * infrastructure failures; INVALID is a data-contract violation; SKIPPED is an explicit
 * non-processing outcome.
 */
public enum ProcessingStatus {

	ACCEPTED, REJECTED_RULE_A, REJECTED_RULE_B, INVALID, SKIPPED

}
