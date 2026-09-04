package dev.thehenrylabs.springbatchcronjob.partition;

import java.util.List;

/** O(partition-count) output of the one sequential validation/indexing pass. */
public record CsvFileIndex(long fileSize, long dataStartOffset, long expectedRecordCount, String sha256,
		List<CsvPartitionDefinition> partitions) {
}
