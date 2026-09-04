package dev.thehenrylabs.springbatchcronjob.partition;

import java.nio.channels.SeekableByteChannel;

/**
 * Produces only safe logical CSV boundaries; physical lines are never used as a proxy.
 */
public interface CsvBoundaryIndexer {

	CsvFileIndex index(SeekableByteChannel channel, PartitionPlan plan);

}
