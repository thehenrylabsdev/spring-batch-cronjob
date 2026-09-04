package dev.thehenrylabs.springbatchcronjob.configuration;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

/** Settings for the PostgreSQL-coordinated execution plane. */
@ConfigurationProperties(prefix = "batch.distributed")
public record DistributedBatchProperties(boolean enabled, Worker worker, Partition partition,
		Duration noWorkerTimeout) {

	public record Worker(int concurrency, Duration heartbeatInterval, Duration activeTimeout,
			Duration idlePollInterval) {
	}

	public record Partition(int partitionsPerActiveWorker, int maxPods, int scaleHeadroomPartitionsPerMaxPod,
			int maxPartitions, DataSize minPartitionBytes, Duration leaseDuration, Duration leaseHeartbeat,
			int maxAttempts) {
	}

}
