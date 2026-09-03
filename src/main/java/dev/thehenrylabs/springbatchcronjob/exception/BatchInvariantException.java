package dev.thehenrylabs.springbatchcronjob.exception;

/**
 * Programming / invariant failure (SPEC 34.4): impossible state, corrupted configuration,
 * unexplained counters. Fail fast — never silently skip.
 */
public class BatchInvariantException extends RuntimeException {

	public BatchInvariantException(String message) {
		super(message);
	}

	public BatchInvariantException(String message, Throwable cause) {
		super(message, cause);
	}

}
