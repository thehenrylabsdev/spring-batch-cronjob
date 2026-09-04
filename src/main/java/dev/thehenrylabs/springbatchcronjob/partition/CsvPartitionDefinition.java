package dev.thehenrylabs.springbatchcronjob.partition;

/**
 * Half-open byte and logical-record range, both ending at a validated record boundary.
 */
public record CsvPartitionDefinition(int partitionNo, long startByte, long endByteExclusive, long startRecordNo,
		long endRecordNoExclusive) {
	public long expectedRecordCount() {
		return this.endRecordNoExclusive - this.startRecordNo;
	}
}
