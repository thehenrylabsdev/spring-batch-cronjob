package dev.thehenrylabs.springbatchcronjob.job.validation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;

import dev.thehenrylabs.springbatchcronjob.TestcontainersConfiguration;
import dev.thehenrylabs.springbatchcronjob.exception.InvalidInputFileException;
import dev.thehenrylabs.springbatchcronjob.file.FileChecksumService;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * Phase-3 tests (SPEC 61.3 + Phase-3 list): the validation tasklet through a real Spring
 * Batch step against Testcontainers PostgreSQL — valid file, invalid header, missing
 * manifest, empty file, and the immutable copy rule.
 */
@Import({ TestcontainersConfiguration.class, InputValidationFlowTests.ValidationJobConfig.class })
@SpringBootTest
class InputValidationFlowTests {

	private static final String VALID_HEADER = "customer_id,field_a,field_b,amount,event_date";

	private static final LocalDate WEEK_1 = LocalDate.of(2026, 9, 7);

	private static final LocalDate WEEK_2 = LocalDate.of(2026, 9, 14);

	private static final LocalDate WEEK_3 = LocalDate.of(2026, 9, 21);

	private static final Path INPUT_DIR = createTempDir("batch-incoming");

	private static final Path PROCESSING_DIR = createTempDir("batch-processing");

	@DynamicPropertySource
	static void inputProperties(DynamicPropertyRegistry registry) {
		registry.add("batch.input.location", () -> INPUT_DIR.toString());
		registry.add("batch.input.processing-location", () -> PROCESSING_DIR.toString());
	}

	@TestConfiguration
	static class ValidationJobConfig {

		@Bean
		Job weeklyCsvProcessingJob(JobRepository jobRepository, PlatformTransactionManager transactionManager,
				InputValidationTasklet tasklet) {
			return new JobBuilder(BatchLaunchService.JOB_NAME, jobRepository)
				.start(new StepBuilder("validateInputStep", jobRepository).tasklet(tasklet, transactionManager).build())
				.build();
		}

	}

	@Autowired
	private BatchLaunchService launchService;

	@Autowired
	private JobOperator jobOperator;

	@Autowired
	private JobRegistry jobRegistry;

	@Autowired
	private FileChecksumService checksumService;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@BeforeEach
	void cleanState() throws Exception {
		this.jdbcTemplate.update("TRUNCATE control.batch_file_manifest RESTART IDENTITY CASCADE");
		this.jdbcTemplate.update("""
				TRUNCATE batch.batch_step_execution_context, batch.batch_job_execution_context,
				batch.batch_step_execution, batch.batch_job_execution_params, batch.batch_job_execution,
				batch.batch_job_instance CASCADE
				""");
		clearDirectory(INPUT_DIR);
		clearDirectory(PROCESSING_DIR);
	}

	@Test
	void validFileReachesReadyViaImmutableCopy() throws Exception {
		createInput(WEEK_1, VALID_HEADER + "\nU001,a,b,10.50,2026-08-31\nU002,c,d,20.00,2026-09-01\n");

		BatchLaunchService.LaunchResult result = this.launchService.launchWeekly(WEEK_1);
		assertThat(result.outcome()).isEqualTo(BatchLaunchService.Outcome.STARTED);
		awaitJobStatus("COMPLETED");

		String status = manifestStatus(WEEK_1);
		assertThat(status).isEqualTo("READY");

		// Immutable content-addressed copy installed; incoming file consumed.
		Path copy = onlyFileIn(PROCESSING_DIR);
		assertThat(copy.getFileName().toString()).startsWith("customers-").endsWith(".csv");
		assertThat(this.checksumService.sha256(copy)).hasSize(64);
		assertThat(Files.exists(fileFor(WEEK_1, INPUT_DIR))).isFalse();
	}

	@Test
	void invalidHeaderFailsValidationAndMarksManifestFailed() throws Exception {
		createInput(WEEK_2, "wrong_id,other\nA,1\n");

		this.launchService.launchWeekly(WEEK_2);
		awaitJobStatus("FAILED");

		assertThat(manifestStatus(WEEK_2)).isEqualTo("FAILED");
	}

	@Test
	void missingManifestFailsTheJob() throws Exception {
		createInput(WEEK_3, VALID_HEADER + "\nU001,a,b,10.50,2026-08-31\n");

		// Manifest bypass: direct start with identity parameters (SPEC 39 layer 2 guard).
		Path file = fileFor(WEEK_3, PROCESSING_DIR);
		Files.copy(fileFor(WEEK_3, INPUT_DIR), file);
		String checksum = this.checksumService.sha256(file);
		var parameters = new org.springframework.batch.core.job.parameters.JobParametersBuilder()
			.addString(BatchLaunchService.PARAM_BUSINESS_DATE, WEEK_3.toString())
			.addString(BatchLaunchService.PARAM_FILE_CHECKSUM, checksum)
			.addString(BatchLaunchService.PARAM_FILE_URI, file.toUri().toString())
			.toJobParameters();
		this.jobOperator.start(this.jobRegistry.getJob(BatchLaunchService.JOB_NAME), parameters);
		awaitJobStatus("FAILED");

		// No manifest row may be created without the manifest layer.
		Integer rows = this.jdbcTemplate.queryForObject(
				"SELECT count(*) FROM control.batch_file_manifest WHERE business_date = ?", Integer.class, WEEK_3);
		assertThat(rows).isEqualTo(0);
	}

	@Test
	void emptyFileIsRejectedBeforeLaunch() throws Exception {
		createInput(WEEK_1, "");
		assertThatThrownBy(() -> this.launchService.launchWeekly(WEEK_1)).isInstanceOf(InvalidInputFileException.class);
	}

	private String manifestStatus(LocalDate businessDate) {
		return this.jdbcTemplate.queryForObject(
				"SELECT status FROM control.batch_file_manifest WHERE business_date = ?", String.class, businessDate);
	}

	private void awaitJobStatus(String status) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 15_000;
		while (System.currentTimeMillis() < deadline) {
			String current = this.jdbcTemplate.queryForObject(
					"SELECT status FROM batch.batch_job_execution ORDER BY job_execution_id DESC LIMIT 1",
					String.class);
			if (status.equals(current)) {
				return;
			}
			Thread.sleep(150);
		}
		throw new AssertionError("Job did not reach " + status);
	}

	private static Path createTempDir(String prefix) {
		try {
			return Files.createTempDirectory(prefix);
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static Path fileFor(LocalDate businessDate, Path dir) {
		return dir.resolve(
				"customers-" + businessDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")) + ".csv");
	}

	private static Path onlyFileIn(Path dir) throws Exception {
		try (var stream = Files.list(dir)) {
			return stream.findFirst().orElseThrow(() -> new AssertionError("no file in " + dir));
		}
	}

	private static void createInput(LocalDate businessDate, String content) throws Exception {
		Files.writeString(fileFor(businessDate, INPUT_DIR), content);
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
