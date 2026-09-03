package dev.thehenrylabs.springbatchcronjob.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import dev.thehenrylabs.springbatchcronjob.TestcontainersConfiguration;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * Performance qualification entry point (SPEC 56, 61.8). Tagged {@code perf} so the
 * default build stays fast:
 *
 * <pre>
 * ./mvnw test -Dgroups=perf -Dperf.records=1000000
 * </pre>
 *
 * Defaults to 1M records; the 10M release gate run uses {@code -Dperf.records=10000000}.
 * Results are appended to {@code target/perf-results.csv}; documented defaults live in
 * {@code docs/benchmarks/defaults.md}.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
@Tag("perf")
class PerformanceQualificationTest {

	private static final Path INPUT_DIR = tempDir("perf-incoming");

	private static final Path PROCESSING_DIR = tempDir("perf-processing");

	@DynamicPropertySource
	static void inputProperties(DynamicPropertyRegistry registry) {
		registry.add("batch.input.location", () -> INPUT_DIR.toString());
		registry.add("batch.input.processing-location", () -> PROCESSING_DIR.toString());
	}

	@Autowired
	private BatchLaunchService launchService;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@BeforeEach
	void clean() throws Exception {
		this.jdbcTemplate.update("TRUNCATE control.batch_file_manifest RESTART IDENTITY CASCADE");
		this.jdbcTemplate.update("TRUNCATE control.batch_run_summary RESTART IDENTITY CASCADE");
		this.jdbcTemplate.update("""
				TRUNCATE batch.batch_step_execution_context, batch.batch_job_execution_context,
				batch.batch_step_execution, batch.batch_job_execution_params, batch.batch_job_execution,
				batch.batch_job_instance CASCADE
				""");
		clear(INPUT_DIR);
		clear(PROCESSING_DIR);
	}

	@Test
	void representativeRunHitsThroughputBaseline() throws Exception {
		int records = Integer.parseInt(System.getProperty("perf.records", "1000000"));
		CsvDataGenerator.write(INPUT_DIR, "customers-20260907.csv", records, 42);

		long started = System.nanoTime();
		BatchLaunchService.LaunchResult launch = this.launchService.launchWeekly(java.time.LocalDate.of(2026, 9, 7));
		assertThat(launch.outcome()).isEqualTo(BatchLaunchService.Outcome.STARTED);
		awaitStatus("COMPLETED", 30_000);
		long elapsedSeconds = Math.max(1, (System.nanoTime() - started) / 1_000_000_000L);
		double recordsPerSecond = (double) records / elapsedSeconds;

		String status = this.jdbcTemplate.queryForObject(
				"SELECT status FROM control.batch_file_manifest WHERE business_date = '2026-09-07'", String.class);
		assertThat(status).isEqualTo("COMPLETED");
		Long recordCount = this.jdbcTemplate.queryForObject(
				"SELECT record_count FROM control.batch_file_manifest WHERE business_date = '2026-09-07'", Long.class);
		assertThat(recordCount).isEqualTo(records);

		appendResult(records, elapsedSeconds, recordsPerSecond);
		// CI-safe floor; real numbers live in docs/benchmarks (SPEC 56).
		assertThat(recordsPerSecond).isGreaterThan(1000);
	}

	private void awaitStatus(String status, int timeoutMs) throws InterruptedException {
		long deadline = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < deadline) {
			String current = this.jdbcTemplate.queryForObject(
					"SELECT status FROM batch.batch_job_execution ORDER BY job_execution_id DESC LIMIT 1",
					String.class);
			if (status.equals(current)) {
				return;
			}
			Thread.sleep(250);
		}
		throw new AssertionError("Job did not reach " + status);
	}

	private static void appendResult(int records, long elapsedSeconds, double rps) throws Exception {
		Path results = Path.of("target/perf-results.csv");
		if (!Files.exists(results)) {
			Files.writeString(results, "timestamp,records,elapsed_seconds,records_per_second\n");
		}
		Files.writeString(results, java.time.LocalDateTime.now() + "," + records + "," + elapsedSeconds + ","
				+ String.format("%.0f", rps) + "\n", java.nio.file.StandardOpenOption.APPEND);
	}

	private static Path tempDir(String prefix) {
		try {
			return Files.createTempDirectory(prefix);
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static void clear(Path dir) throws Exception {
		try (var stream = Files.list(dir)) {
			stream.forEach(path -> {
				try {
					Files.deleteIfExists(path);
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		}
	}

}
