package dev.thehenrylabs.springbatchcronjob.domain;

/**
 * Deterministic, side-effect-free classification (SPEC 24). Implementations MUST be
 * thread-safe and independent of scheduling concerns.
 */
public interface RecordClassifier<I, O> {

	O classify(I input);

}
