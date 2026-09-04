package dev.thehenrylabs.springbatchcronjob.domain;

import java.time.Instant;

/**
 * PostgreSQL-backed worker presence record; pod count is deliberately not a source of
 * truth.
 */
public record BatchWorker(String workerId, String podName, String podNamespace, String applicationVersion,
		WorkerStatus status, int workerSlots, Instant startedAt, Instant lastHeartbeatAt, Long currentJobExecution,
		Long currentPartitionId, boolean shutdownRequested) {
}
