package dev.thehenrylabs.springbatchcronjob.configuration;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed application configuration for the weekly CSV batch pipeline (SPEC 43).
 *
 * <p>
 * Only application-owned settings live here; framework configuration that Spring Boot
 * already exposes (e.g. {@code spring.quartz.*}) is kept in {@code application.yaml}
 * rather than duplicated. All values are supplied by configuration; the reference default
 * set is documented in {@code application.yaml}.
 *
 * @param schedule weekly scheduling settings (Quartz trigger)
 * @param input input file contract settings
 * @param processing chunk-oriented processing settings
 * @param retry bounded, exception-specific retry settings
 */
@ConfigurationProperties(prefix = "batch")
public record BatchProperties(Schedule schedule, Input input, Processing processing, Retry retry) {

	/** Weekly schedule settings (SPEC FR-001, FR-005). */
	public record Schedule(boolean enabled, String cron, String zone, MisfirePolicy misfirePolicy) {
	}

	/** Input file contract (SPEC 10, 13). */
	public record Input(String resource, java.time.Duration stabilityWindow, String location, String processingLocation,
			List<String> expectedHeader, String expectedDelimiter, String expectedEncoding, String fileNamePattern,
			long minSizeBytes, long maxSizeBytes, String expectedChecksum) {
	}

	/** Chunk-oriented processing settings (SPEC 21, 28, 36). */
	public record Processing(int chunkSize, int skipLimit, int progressLogRecords, int progressLogIntervalSeconds,
			Executor executor, Partitioning partitioning) {

		/** Bounded dedicated batch executor (SPEC 29). */
		public record Executor(int corePoolSize, int maxPoolSize, int queueCapacity) {
		}

		/**
		 * Scaling seam; Level 1 default until benchmark evidence dictates otherwise (SPEC
		 * 8).
		 */
		public record Partitioning(PartitioningMode mode, int gridSize) {
		}
	}

	/** Bounded retry policy for transient infrastructure failures (SPEC 35). */
	public record Retry(int maxAttempts, Duration initialBackoff, double multiplier, Duration maxBackoff) {
	}

	/** Supported Quartz misfire policies for the weekly trigger (SPEC FR-005). */
	public enum MisfirePolicy {

		FIRE_ONCE_NOW, DO_NOTHING

	}

	/** Current processing strategy; Level 1 is the default (SPEC 8). */
	public enum PartitioningMode {

		SINGLE_READER, SAFE_BOUNDARIES, SHARDS

	}
}
