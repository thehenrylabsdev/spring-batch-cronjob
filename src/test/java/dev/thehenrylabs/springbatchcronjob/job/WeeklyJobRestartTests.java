package dev.thehenrylabs.springbatchcronjob.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
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
 * End-to-end distributed processing test: a 10,000-record input is partitioned, claimed,
 * processed, and reconciled exactly once.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class WeeklyJobRestartTests {

	private static final int TOTAL_RECORDS = 10_000;

	private static final Path INPUT_DIR = createTempDir("restart-incoming");

	@DynamicPropertySource
	static void inputProperties(DynamicPropertyRegistry registry) {
		registry.add("batch.input.location", () -> INPUT_DIR.toString());
	}

	@Autowired
	private BatchLaunchService launchService;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@BeforeEach
	void cleanState() throws Exception {
		this.jdbcTemplate.update("TRUNCATE control.batch_file_manifest RESTART IDENTITY CASCADE");
		this.jdbcTemplate.update("TRUNCATE control.batch_run_summary RESTART IDENTITY CASCADE");
		this.jdbcTemplate.update("""
				TRUNCATE batch.batch_step_execution_context, batch.batch_job_execution_context,
				batch.batch_step_execution, batch.batch_job_execution_params, batch.batch_job_execution,
				batch.batch_job_instance CASCADE
				""");
		clearDirectory(INPUT_DIR);
	}

	@Test
	void processesPartitionedFileToExactCounts() throws Exception {
		createInput(TOTAL_RECORDS);
		long expectedInvalid = countInvalid(TOTAL_RECORDS);
		long expectedSkipped = countSkipped(TOTAL_RECORDS);
		long expectedAccepted = TOTAL_RECORDS - expectedInvalid - expectedSkipped;

		BatchLaunchService.LaunchResult launch = this.launchService.launchWeekly(java.time.LocalDate.of(2026, 9, 7));
		assertThat(launch.outcome()).isEqualTo(BatchLaunchService.Outcome.STARTED);
		awaitJobStatus("COMPLETED");

		// Exactly one JobInstance and execution processed the manifest.
		Integer instances = this.jdbcTemplate.queryForObject("SELECT count(*) FROM batch.batch_job_instance",
				Integer.class);
		assertThat(instances).isEqualTo(1);
		Integer executions = this.jdbcTemplate.queryForObject("SELECT count(*) FROM batch.batch_job_execution",
				Integer.class);
		assertThat(executions).isEqualTo(1);

		// Manifest COMPLETED with the exact total; no double counting.
		Long recordCount = this.jdbcTemplate.queryForObject(
				"SELECT record_count FROM control.batch_file_manifest WHERE business_date = '2026-09-07'", Long.class);
		assertThat(recordCount).isEqualTo(TOTAL_RECORDS);

		// Final counts reconcile exactly.
		Long accepted = this.jdbcTemplate.queryForObject("SELECT accepted_count FROM control.batch_run_summary",
				Long.class);
		Long invalid = this.jdbcTemplate.queryForObject("SELECT invalid_count FROM control.batch_run_summary",
				Long.class);
		Long skipped = this.jdbcTemplate.queryForObject("SELECT skipped_count FROM control.batch_run_summary",
				Long.class);
		Long read = this.jdbcTemplate.queryForObject("SELECT read_count FROM control.batch_run_summary", Long.class);
		Long rejected = this.jdbcTemplate.queryForObject("SELECT rejected_count FROM control.batch_run_summary",
				Long.class);
		assertThat(invalid).isEqualTo(expectedInvalid);
		assertThat(skipped).isEqualTo(expectedSkipped);
		assertThat(accepted).isEqualTo(expectedAccepted);
		assertThat(rejected).isZero();
		assertThat(read).isEqualTo(accepted + rejected + invalid + skipped);
	}

	/** Rows where i % 10 == 0 have an unparseable amount -> INVALID. */
	private static long countInvalid(int total) {
		return total / 10;
	}

	/** Rows where i % 7 == 0 (and not already invalid) have fieldA = S -> SKIPPED. */
	private static long countSkipped(int total) {
		long out = 0;
		for (int i = 1; i <= total; i++) {
			if (i % 7 == 0 && i % 10 != 0) {
				out++;
			}
		}
		return out;
	}

	private static void createInput(int records) throws Exception {
		StringBuilder content = new StringBuilder("customer_id,field_a,field_b,amount,event_date\n");
		for (int i = 1; i <= records; i++) {
			String customerId = "U" + String.format("%03d", (i - 1) % 500 + 1);
			String fieldA = i % 7 == 0 ? "S" : "a";
			String amount = i % 10 == 0 ? "x" : "12.34";
			content.append(customerId).append(',').append(fieldA).append(",b,").append(amount).append(",2026-08-31\n");
		}
		Files.writeString(INPUT_DIR.resolve("customers-20260907.csv"), content.toString());
	}

	private void awaitJobStatus(String status) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 30_000;
		while (System.currentTimeMillis() < deadline) {
			String current = this.jdbcTemplate.queryForObject(
					"SELECT status FROM batch.batch_job_execution ORDER BY job_execution_id DESC LIMIT 1",
					String.class);
			if (status.equals(current)) {
				return;
			}
			Thread.sleep(200);
		}
		var executions = this.jdbcTemplate.queryForList(
				"SELECT job_execution_id, status, exit_message FROM batch.batch_job_execution ORDER BY job_execution_id");
		var steps = this.jdbcTemplate.queryForList(
				"SELECT job_execution_id, step_name, status, exit_message FROM batch.batch_step_execution ORDER BY job_execution_id");
		throw new AssertionError("Job did not reach " + status + "; executions=" + executions + "; steps=" + steps);
	}

	private static Path createTempDir(String prefix) {
		try {
			return Files.createTempDirectory(prefix);
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static void clearDirectory(Path dir) throws Exception {
		if (Files.exists(dir)) {
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

}
