package dev.thehenrylabs.springbatchcronjob.partition;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class Utf8CsvBoundaryIndexerTests {

	private final Utf8CsvBoundaryIndexer indexer = new Utf8CsvBoundaryIndexer();

	@Test
	void preservesMultilineQuotedRecordsAndCoverage() throws Exception {
		Path file = Files.createTempFile("boundary", ".csv");
		Files.writeString(file, "id,name,note\r\n1,Henry,one\r\n2,Jane,\"line one\nline two\"\r\n3,Bob,done\r\n");
		try (SeekableByteChannel channel = Files.newByteChannel(file)) {
			CsvFileIndex index = this.indexer.index(channel, new PartitionPlan(3));
			assertThat(index.expectedRecordCount()).isEqualTo(3);
			assertThat(index.partitions()).isNotEmpty();
			assertThat(index.partitions().getFirst().startByte()).isEqualTo(index.dataStartOffset());
			assertThat(index.partitions().getLast().endByteExclusive()).isEqualTo(index.fileSize());
			assertThat(index.partitions())
				.allSatisfy(partition -> assertThat(partition.expectedRecordCount()).isPositive());
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

}
