package dev.thehenrylabs.springbatchcronjob.domain;

import java.time.Instant;

/** Claimed work unit. leaseVersion is the fencing token for every mutable update. */
public record BatchPartition(long id, long manifestId, long jobExecutionId, Long stepExecutionId, String stepName,
		int partitionNo, long startByte, long endByteExclusive, long startRecordNo, long endRecordNoExclusive,
		long expectedRecordCount, PartitionStatus status, String workerId, long leaseVersion, Instant leaseExpiresAt,
		long checkpointByte, long checkpointRecordNo, long committedReadCount, long committedProcessedCount,
		long acceptedCount, long rejectedCount, long invalidCount, long skippedCount, int attemptCount) {
}
