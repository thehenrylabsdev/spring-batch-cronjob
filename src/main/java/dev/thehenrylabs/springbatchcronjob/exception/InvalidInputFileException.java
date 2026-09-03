package dev.thehenrylabs.springbatchcronjob.exception;

/**
 * Input file contract violation (SPEC 13). The job must stop before the processing phase
 * begins; configuration is treated as an application error, not an infrastructure error.
 */
public class InvalidInputFileException extends RuntimeException {

	public InvalidInputFileException(String message) {
		super(message);
	}

	public InvalidInputFileException(String message, Throwable cause) {
		super(message, cause);
	}

}
