package dev.thehenrylabs.springbatchcronjob.service;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dev.thehenrylabs.springbatchcronjob.configuration.DistributedBatchProperties;
import dev.thehenrylabs.springbatchcronjob.domain.BatchPartition;
import dev.thehenrylabs.springbatchcronjob.exception.PartitionLeaseLostException;
import dev.thehenrylabs.springbatchcronjob.repository.DistributedPartitionRepository;

/**
 * Service boundary which turns every zero-row fenced mutation into an immediate abort.
 */
@Service
public class PartitionLeaseService {

	private final DistributedPartitionRepository partitions;

	private final DistributedBatchProperties properties;

	private final Clock clock;

	public PartitionLeaseService(DistributedPartitionRepository partitions, DistributedBatchProperties properties,
			Clock clock) {
		this.partitions = partitions;
		this.properties = properties;
		this.clock = clock;
	}

	@Transactional
	public Optional<BatchPartition> claim(long jobExecutionId, String workerId) {
		Instant now = this.clock.instant();
		return this.partitions.claim(jobExecutionId, workerId, now.plus(this.properties.partition().leaseDuration()),
				now, this.properties.partition().maxAttempts());
	}

	@Transactional
	public Optional<BatchPartition> claimAny(String workerId) {
		Instant now = this.clock.instant();
		return this.partitions.claimAny(workerId, now.plus(this.properties.partition().leaseDuration()), now,
				this.properties.partition().maxAttempts());
	}

	public void start(BatchPartition partition) {
		guard(partition.id(), this.partitions.start(partition.id(), partition.workerId(), partition.leaseVersion(),
				expires(), this.clock.instant()));
	}

	public void renew(BatchPartition partition) {
		guard(partition.id(), this.partitions.renew(partition.id(), partition.workerId(), partition.leaseVersion(),
				expires(), this.clock.instant()));
	}

	public void release(BatchPartition partition) {
		guard(partition.id(), this.partitions.release(partition.id(), partition.workerId(), partition.leaseVersion(),
				this.clock.instant()));
	}

	public void checkpoint(BatchPartition partition, long byteOffset, long recordNo, long read, long processed,
			long accepted, long rejected, long invalid) {
		guard(partition.id(), this.partitions.checkpoint(partition.id(), partition.workerId(), partition.leaseVersion(),
				byteOffset, recordNo, read, processed, accepted, rejected, invalid, this.clock.instant()));
	}

	public void complete(BatchPartition partition) {
		guard(partition.id(), this.partitions.complete(partition.id(), partition.workerId(), partition.leaseVersion(),
				this.clock.instant()));
	}

	public int reapExpired() {
		return this.partitions.reapExpired(this.clock.instant());
	}

	private Instant expires() {
		return this.clock.instant().plus(this.properties.partition().leaseDuration());
	}

	private static void guard(long id, int rows) {
		if (rows != 1)
			throw new PartitionLeaseLostException(id);
	}

}
