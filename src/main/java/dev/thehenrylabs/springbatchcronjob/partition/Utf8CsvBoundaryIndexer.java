package dev.thehenrylabs.springbatchcronjob.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;

/**
 * RFC-4180-compatible raw UTF-8 byte scanner. ASCII delimiter/quote bytes are UTF-8
 * invariant.
 */
@Component
public class Utf8CsvBoundaryIndexer implements CsvBoundaryIndexer {

	private enum State {

		OUTSIDE_QUOTED_FIELD, INSIDE_QUOTED_FIELD, AFTER_QUOTE

	}

	@Override
	public CsvFileIndex index(SeekableByteChannel channel, PartitionPlan plan) {
		try {
			long fileSize = channel.size();
			if (fileSize == 0)
				throw new InvalidInputFileException("CSV is empty");
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
			State state = State.OUTSIDE_QUOTED_FIELD;
			long offset = 0, headerEnd = -1, records = 0, startByte = -1, startRecord = 0;
			long nextTarget = Math.max(1, fileSize / plan.targetPartitions());
			List<CsvPartitionDefinition> partitions = new ArrayList<>();
			channel.position(0);
			while (channel.read(buffer) != -1) {
				buffer.flip();
				byte[] bytes = new byte[buffer.remaining()];
				buffer.get(bytes);
				digest.update(bytes);
				for (byte value : bytes) {
					offset++;
					int c = value & 0xff;
					state = transition(state, c);
					if (c == '\n' && state == State.OUTSIDE_QUOTED_FIELD) {
						if (headerEnd < 0) {
							headerEnd = offset;
							startByte = offset;
						}
						else {
							records++;
							if (partitions.size() < plan.targetPartitions() - 1 && offset >= nextTarget) {
								partitions.add(new CsvPartitionDefinition(partitions.size(), startByte, offset,
										startRecord, records));
								startByte = offset;
								startRecord = records;
								nextTarget = Math.max(nextTarget + 1,
										fileSize * (partitions.size() + 1L) / plan.targetPartitions());
							}
						}
					}
				}
				buffer.clear();
			}
			if (state == State.INSIDE_QUOTED_FIELD)
				throw new InvalidInputFileException("CSV has an unclosed quoted field");
			if (headerEnd < 0)
				throw new InvalidInputFileException("CSV has no complete header record");
			if (offset > headerEnd && !endsAtNewline(channel, offset))
				records++;
			if (startByte < fileSize || records > startRecord)
				partitions
					.add(new CsvPartitionDefinition(partitions.size(), startByte, fileSize, startRecord, records));
			validate(fileSize, headerEnd, records, partitions);
			return new CsvFileIndex(fileSize, headerEnd, records, HexFormat.of().formatHex(digest.digest()),
					List.copyOf(partitions));
		}
		catch (IOException | NoSuchAlgorithmException e) {
			throw new InvalidInputFileException("Cannot index CSV", e);
		}
	}

	private static State transition(State state, int c) {
		return switch (state) {
			case OUTSIDE_QUOTED_FIELD -> c == '"' ? State.INSIDE_QUOTED_FIELD : State.OUTSIDE_QUOTED_FIELD;
			case INSIDE_QUOTED_FIELD -> c == '"' ? State.AFTER_QUOTE : State.INSIDE_QUOTED_FIELD;
			case AFTER_QUOTE -> c == '"' ? State.INSIDE_QUOTED_FIELD : State.OUTSIDE_QUOTED_FIELD;
		};
	}

	private static boolean endsAtNewline(SeekableByteChannel channel, long size) throws IOException {
		ByteBuffer one = ByteBuffer.allocate(1);
		channel.position(size - 1);
		channel.read(one);
		return one.array()[0] == '\n';
	}

	private static void validate(long size, long dataStart, long records, List<CsvPartitionDefinition> partitions) {
		long byteCursor = dataStart, recordCursor = 0;
		for (CsvPartitionDefinition p : partitions) {
			if (p.startByte() != byteCursor || p.startRecordNo() != recordCursor || p.endByteExclusive() < p.startByte()
					|| p.endRecordNoExclusive() < p.startRecordNo())
				throw new InvalidInputFileException("CSV partition coverage invariant failed");
			byteCursor = p.endByteExclusive();
			recordCursor = p.endRecordNoExclusive();
		}
		if (byteCursor != size || recordCursor != records)
			throw new InvalidInputFileException("CSV partition coverage is incomplete");
	}

}
