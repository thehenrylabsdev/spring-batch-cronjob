package dev.thehenrylabs.springbatchcronjob.service;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;

import org.springframework.stereotype.Service;

import dev.thehenrylabs.springbatchcronjob.configuration.DistributedBatchProperties;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;
import dev.thehenrylabs.springbatchcronjob.partition.CsvFileIndex;
import dev.thehenrylabs.springbatchcronjob.partition.CsvBoundaryIndexer;
import dev.thehenrylabs.springbatchcronjob.partition.PartitionPlan;
import dev.thehenrylabs.springbatchcronjob.repository.DistributedPartitionRepository;
import dev.thehenrylabs.springbatchcronjob.repository.DistributedWorkerRepository;

/** Performs the one coordinator-only scan and persists its immutable work queue. */
@Service
public class PartitionPlanningService {

	private final DistributedWorkerRepository workers;

	private final DistributedPartitionRepository partitions;

	private final CsvBoundaryIndexer indexer;

	private final DistributedBatchProperties properties;

	private final Clock clock;

	public PartitionPlanningService(DistributedWorkerRepository workers, DistributedPartitionRepository partitions,
			CsvBoundaryIndexer indexer, DistributedBatchProperties properties, Clock clock) {
		this.workers = workers;
		this.partitions = partitions;
		this.indexer = indexer;
		this.properties = properties;
		this.clock = clock;
	}

	public CsvFileIndex indexAndPersist(long manifestId, long executionId, URI resource) {
		Path path = Path.of(resource);
		long sizeBefore = size(path);
		Instant modifiedBefore = modified(path);
		int slots = this.workers.activeSlots(this.clock.instant().minus(this.properties.worker().activeTimeout()));
		int target = targetPartitions(slots, sizeBefore);
		CsvFileIndex index;
		try (SeekableByteChannel channel = Files.newByteChannel(path)) {
			index = this.indexer.index(channel, new PartitionPlan(target));
		}
		catch (java.io.IOException e) {
			throw new InvalidInputFileException("Cannot open input for indexing " + path, e);
		}
		if (sizeBefore != size(path) || !modifiedBefore.equals(modified(path))) {
			throw new InvalidInputFileException("Input changed during CSV indexing: " + path);
		}
		this.partitions.create(manifestId, executionId, index.partitions(), this.clock.instant());
		return index;
	}

	public int targetPartitions(int activeSlots, long bytes) {
		var p = this.properties.partition();
		int candidate = Math.max(activeSlots * p.partitionsPerActiveWorker(),
				p.maxPods() * p.scaleHeadroomPartitionsPerMaxPod());
		int bounded = Math.max(1, Math.min(candidate, p.maxPartitions()));
		long minimum = Math.max(1L, p.minPartitionBytes().toBytes());
		return (int) Math.max(1, Math.min(bounded, Math.max(1L, bytes / minimum)));
	}

	private static long size(Path path) {
		try {
			return Files.size(path);
		}
		catch (java.io.IOException e) {
			throw new InvalidInputFileException("Cannot stat " + path, e);
		}
	}

	private static Instant modified(Path path) {
		try {
			return Files.getLastModifiedTime(path).toInstant();
		}
		catch (java.io.IOException e) {
			throw new InvalidInputFileException("Cannot stat " + path, e);
		}
	}

}
