package dev.thehenrylabs.springbatchcronjob.service;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.configuration.DistributedBatchProperties;
import dev.thehenrylabs.springbatchcronjob.configuration.WorkerIdentity;
import dev.thehenrylabs.springbatchcronjob.domain.BatchFileManifest;
import dev.thehenrylabs.springbatchcronjob.domain.BatchPartition;
import dev.thehenrylabs.springbatchcronjob.domain.CustomerCsvRecord;
import dev.thehenrylabs.springbatchcronjob.domain.ProcessingResult;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.job.processing.ByteRangeCsvItemReader;
import dev.thehenrylabs.springbatchcronjob.job.processing.CsvRecordProcessor;
import dev.thehenrylabs.springbatchcronjob.repository.BatchFileManifestRepository;

/**
 * The durable database queue provides work stealing; this loop intentionally owns one
 * slot.
 */
@Component
public class DistributedWorkerPollingService implements SmartLifecycle {

	private final PartitionLeaseService leases;

	private final BatchFileManifestRepository manifests;

	private final CsvRecordProcessor processor;

	private final DistributedWorkerLifecycle lifecycle;

	private final WorkerIdentity identity;

	private final DistributedBatchProperties distributed;

	private final BatchProperties batch;

	private ScheduledExecutorService executor;

	private volatile boolean running;

	public DistributedWorkerPollingService(PartitionLeaseService leases, BatchFileManifestRepository manifests,
			CsvRecordProcessor processor, DistributedWorkerLifecycle lifecycle, WorkerIdentity identity,
			DistributedBatchProperties distributed, BatchProperties batch) {
		this.leases = leases;
		this.manifests = manifests;
		this.processor = processor;
		this.lifecycle = lifecycle;
		this.identity = identity;
		this.distributed = distributed;
		this.batch = batch;
	}

	@Override
	public void start() {
		if (!this.distributed.enabled() || this.running)
			return;
		this.running = true;
		this.executor = Executors.newScheduledThreadPool(this.distributed.worker().concurrency(),
				Thread.ofPlatform().name("partition-worker-", 0).factory());
		for (int slot = 0; slot < this.distributed.worker().concurrency(); slot++) {
			this.executor.scheduleWithFixedDelay(this::pollSafely, 0,
					this.distributed.worker().idlePollInterval().toMillis(), TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void stop() {
		this.running = false;
		if (this.executor != null)
			this.executor.shutdown();
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	private void pollSafely() {
		try {
			poll();
		}
		catch (RuntimeException ignored) {
			this.lifecycle.idle();
		}
	}

	private void poll() {
		this.leases.reapExpired();
		this.leases.claimAny(this.identity.workerId()).ifPresent(this::process);
	}

	private void process(BatchPartition partition) {
		this.lifecycle.working(partition.jobExecutionId(), partition.id());
		try {
			this.leases.start(partition);
			BatchFileManifest manifest = this.manifests.findById(partition.manifestId())
				.orElseThrow(() -> new BatchInvariantException("Missing manifest " + partition.manifestId()));
			Path file = Path.of(URI.create(manifest.fileUri()));
			verifyUnchanged(manifest, file);
			long read = partition.committedReadCount();
			long processed = partition.committedProcessedCount();
			long accepted = partition.acceptedCount();
			long rejected = partition.rejectedCount();
			long invalid = partition.invalidCount();
			ByteRangeCsvItemReader reader = new ByteRangeCsvItemReader(file, partition.checkpointByte(),
					partition.checkpointRecordNo(), partition.endByteExclusive());
			reader.open(new org.springframework.batch.infrastructure.item.ExecutionContext());
			try {
				CustomerCsvRecord record;
				int inChunk = 0;
				while ((record = reader.read()) != null) {
					ProcessingResult result = this.processor.process(record);
					read++;
					processed++;
					inChunk++;
					switch (result.status()) {
						case ACCEPTED -> accepted++;
						case INVALID -> invalid++;
						default -> rejected++;
					}
					if (inChunk >= this.batch.processing().chunkSize()) {
						checkpoint(partition, reader, read, processed, accepted, rejected, invalid);
						this.leases.renew(partition);
						inChunk = 0;
					}
				}
				checkpoint(partition, reader, read, processed, accepted, rejected, invalid);
				if (processed != partition.expectedRecordCount())
					throw new BatchInvariantException("Partition count does not reconcile: " + partition.id());
				verifyUnchanged(manifest, file);
				this.leases.complete(partition);
			}
			finally {
				reader.close();
			}
		}
		catch (RuntimeException e) {
			try {
				this.leases.release(partition);
			}
			catch (RuntimeException ignored) {
			}
			throw e;
		}
		finally {
			this.lifecycle.idle();
		}
	}

	private void checkpoint(BatchPartition partition, ByteRangeCsvItemReader reader, long read, long processed,
			long accepted, long rejected, long invalid) {
		this.leases.checkpoint(partition, reader.nextByteOffset(), reader.nextRecordNumber(), read, processed, accepted,
				rejected, invalid);
	}

	private static void verifyUnchanged(BatchFileManifest manifest, Path file) {
		try {
			if (Files.size(file) != manifest.fileSizeBytes())
				throw new BatchInvariantException("Input size changed during processing");
		}
		catch (java.io.IOException e) {
			throw new BatchInvariantException("Cannot stat input during processing", e);
		}
	}

}
