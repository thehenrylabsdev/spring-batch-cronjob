package dev.thehenrylabs.springbatchcronjob.job.processing;

/**
 * Test seam for deterministic mid-file failure injection (SPEC 61.6). Production wiring
 * resolves the default no-op; a test bean can fail the job exactly at a logical record
 * boundary. Any thrown exception is an uncompensated runtime failure (not a skip), so the
 * step aborts and checkpoint state is preserved.
 */
public interface RecordBoundaryFailureInjector {

	void maybeThrow(long logicalRecordNumber);

	/** Production default: nothing ever fails on record boundaries. */
	final class Noop implements RecordBoundaryFailureInjector {

		@Override
		public void maybeThrow(long logicalRecordNumber) {
			// no-op
		}

	}

}
