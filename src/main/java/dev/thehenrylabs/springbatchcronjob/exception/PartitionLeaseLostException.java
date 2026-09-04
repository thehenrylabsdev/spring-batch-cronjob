package dev.thehenrylabs.springbatchcronjob.exception;

/** A fenced write matched no row: another worker owns the partition now. */
public class PartitionLeaseLostException extends BatchInvariantException {

	public PartitionLeaseLostException(long partitionId) {
		super("Lost lease ownership for partition " + partitionId);
	}

}
