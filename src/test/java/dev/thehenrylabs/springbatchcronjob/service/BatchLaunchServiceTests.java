package dev.thehenrylabs.springbatchcronjob.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import dev.thehenrylabs.springbatchcronjob.TestcontainersConfiguration;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService.LaunchResult;

/**
 * Launch-service tests (SPEC 11, 39): deterministic identity, manifest serialization, and
 * Spring Batch duplicate protection. Uses a trivial job registered under the real job
 * name — the production job replaces it in Phase 4 without touching the launcher.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class BatchLaunchServiceTests {

	private static final LocalDate MONDAY = LocalDate.of(2026, 9, 7);

	private static final Path INPUT_DIR = createInputDirectory();

	private static Path createInputDirectory() {
		try {
			Path dir = Files.createTempDirectory("batch-input");
			Files.writeString(dir.resolve("customers-20260907.csv"), "customer_id,field_a,field_b,amount,event_date\n"
					+ "U001,a,b,10.50,2026-08-31\n" + "U002,c,d,20.00,2026-09-01\n");
			return dir;
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@org.springframework.test.context.DynamicPropertySource
	static void inputLocation(org.springframework.test.context.DynamicPropertyRegistry registry) {
		registry.add("batch.input.location", () -> INPUT_DIR.toString());
		// Same directory: the immutable-copy move is a no-op in launcher-only tests.
		registry.add("batch.input.processing-location", () -> INPUT_DIR.toString());
	}

	@Autowired
	private BatchLaunchService launchService;

	@Autowired
	private JobOperator jobOperator;

	@Autowired
	private JobRegistry jobRegistry;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@BeforeEach
	void cleanTables() {
		this.jdbcTemplate.update("TRUNCATE control.batch_file_manifest RESTART IDENTITY CASCADE");
		this.jdbcTemplate.update("TRUNCATE control.batch_run_summary RESTART IDENTITY CASCADE");
		this.jdbcTemplate.update("""
				TRUNCATE batch.batch_step_execution_context, batch.batch_job_execution_context,
				batch.batch_step_execution, batch.batch_job_execution_params, batch.batch_job_execution,
				batch.batch_job_instance CASCADE
				""");
	}

	@Test
	void missingFileFailsFast() {
		// Resolve failures are propagated (loud failure, SPEC 34.4) rather than
		// classified as a silent ERROR outcome.
		assertThatThrownBy(() -> this.launchService.launchWeekly(LocalDate.of(2026, 9, 14)))
			.isInstanceOf(dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException.class);
	}

	@Test
	void launchesWithDeterministicParametersAndCompletes() throws Exception {
		LaunchResult result = this.launchService.launchWeekly(MONDAY);
		assertThat(result.outcome()).isEqualTo(BatchLaunchService.Outcome.STARTED);
		assertThat(result.jobExecutionId()).isPresent();

		awaitStatus("COMPLETED");
		// Full pipeline: manifest COMPLETED with both records, run summary persisted.
		String manifestStatus = this.jdbcTemplate.queryForObject(
				"SELECT status FROM control.batch_file_manifest WHERE business_date = ?", String.class, MONDAY);
		assertThat(manifestStatus).isEqualTo("COMPLETED");
		Long recordCount = this.jdbcTemplate.queryForObject(
				"SELECT record_count FROM control.batch_file_manifest WHERE business_date = ?", Long.class, MONDAY);
		assertThat(recordCount).isEqualTo(2);
		Integer summaryRows = this.jdbcTemplate.queryForObject(
				"SELECT count(*) FROM control.batch_run_summary WHERE business_date = ?", Integer.class, MONDAY);
		assertThat(summaryRows).isEqualTo(1);

		var params = this.jdbcTemplate.queryForList("""
				SELECT parameter_name, parameter_value
				FROM batch.batch_job_execution_params
				ORDER BY parameter_name
				""");
		assertThat(params).extracting(row -> row.get("parameter_name"))
			.containsExactlyInAnyOrder("businessDate", "fileUri", "fileChecksum");
	}

	@Test
	void secondLaunchIsBlockedByManifestIdentity() {
		this.launchService.launchWeekly(MONDAY);
		LaunchResult second = this.launchService.launchWeekly(MONDAY);
		assertThat(second.outcome()).isEqualTo(BatchLaunchService.Outcome.DUPLICATE_MANIFEST);

		Integer instances = this.jdbcTemplate.queryForObject("SELECT count(*) FROM batch.batch_job_instance",
				Integer.class);
		assertThat(instances).isEqualTo(1);
	}

	@Test
	void completedJobInstanceCannotBeRestartedSilently() throws Exception {
		this.launchService.launchWeekly(MONDAY);
		awaitStatus("COMPLETED");
		Job job = this.jobRegistry.getJob(BatchLaunchService.JOB_NAME);
		// Rebuild the exact identity from persisted parameters: the completed
		// JobInstance must not be started again.
		var stored = this.jdbcTemplate.queryForList(
				"SELECT parameter_name, parameter_value FROM batch.batch_job_execution_params ORDER BY parameter_name");
		JobParametersBuilder builder = new JobParametersBuilder();
		for (Map<String, Object> row : stored) {
			builder.addString((String) row.get("parameter_name"), (String) row.get("parameter_value"));
		}
		try {
			this.jobOperator.start(job, builder.toJobParameters());
		}
		catch (org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException rejected) {
			return; // completed instance rejected by the repository (expected path)
		}
		// Depending on the Batch build, a continuation execution may be created — but the
		// deterministic identity MUST NOT be recreated, and the manifest uniqueness layer
		// prevents the continuation from silently re-completing (SPEC 11/39).
		Integer instances = this.jdbcTemplate.queryForObject("SELECT count(*) FROM batch.batch_job_instance",
				Integer.class);
		assertThat(instances).isEqualTo(1);
	}

	private void awaitStatus(String status) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 10_000;
		while (System.currentTimeMillis() < deadline) {
			String current = this.jdbcTemplate.queryForObject(
					"SELECT status FROM batch.batch_job_execution ORDER BY job_execution_id DESC LIMIT 1",
					String.class);
			if (status.equals(current)) {
				return;
			}
			Thread.sleep(100);
		}
	}

}
