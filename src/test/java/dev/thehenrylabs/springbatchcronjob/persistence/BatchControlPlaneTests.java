package dev.thehenrylabs.springbatchcronjob.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import dev.thehenrylabs.springbatchcronjob.TestcontainersConfiguration;
import dev.thehenrylabs.springbatchcronjob.domain.BatchFileManifest;
import dev.thehenrylabs.springbatchcronjob.domain.ManifestStatus;
import dev.thehenrylabs.springbatchcronjob.exception.BatchInvariantException;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService;
import dev.thehenrylabs.springbatchcronjob.service.FileManifestService.Registration;

/**
 * Phase-1 tests: manifest uniqueness constraint (SPEC 12, 39) and the guarded state
 * machine (SPEC 41), against Testcontainers PostgreSQL.
 */
@Import({ TestcontainersConfiguration.class, BatchControlPlaneTests.FixedClock.class })
@SpringBootTest
class BatchControlPlaneTests {

	private static final LocalDate MONDAY = LocalDate.of(2026, 9, 7);

	private static final Instant FIXED_NOW = Instant.parse("2026-09-07T23:00:00Z");

	@TestConfiguration
	static class FixedClock {

		// Distinct bean name + @Primary so it wins type-based injection without
		// clashing with the production ClockConfiguration bean.
		@Bean
		@Primary
		Clock fixedClock() {
			return Clock.fixed(FIXED_NOW, ZoneOffset.UTC);
		}

	}

	@Autowired
	private FileManifestService manifestService;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@org.junit.jupiter.api.BeforeEach
	void cleanManifests() {
		this.jdbcTemplate.update("DELETE FROM control.batch_file_manifest");
	}

	@Test
	void duplicateIdentityIsRejectedNotDuplicated() {
		Registration first = manifestService.registerDiscovered(MONDAY, "customers_20260907.csv",
				URI.create("file:///data/incoming/customers_20260907.csv"), 1024L, "a".repeat(64));
		assertThat(first.duplicate()).isFalse();

		// Same business date + checksum delivered again -> the constraint is the
		// serialization point; no second row, duplicate flag set.
		Registration second = manifestService.registerDiscovered(MONDAY, "customers_20260907_copy.csv",
				URI.create("file:///data/incoming/customers_20260907_copy.csv"), 2048L, "a".repeat(64));
		assertThat(second.duplicate()).isTrue();
		assertThat(second.manifest().id()).isEqualTo(first.manifest().id());

		Integer count = jdbcTemplate.queryForObject(
				"SELECT count(*) FROM control.batch_file_manifest WHERE business_date = ? AND checksum_sha256 = ?",
				Integer.class, MONDAY, "a".repeat(64));
		assertThat(count).isEqualTo(1);
	}

	@Test
	void differentChecksumIsAValidNewIdentity() {
		manifestService.registerDiscovered(MONDAY, "customers_20260907.csv", URI.create("file:///a.csv"), 1024L,
				"a".repeat(64));
		Registration other = manifestService.registerDiscovered(MONDAY, "customers_20260907.csv",
				URI.create("file:///a.csv"), 1024L, "b".repeat(64));
		assertThat(other.duplicate()).isFalse();
	}

	@Test
	void happyPathStateMachine() {
		Registration registration = manifestService.registerDiscovered(MONDAY, "customers.csv",
				URI.create("file:///data/customers.csv"), 42L, "c".repeat(64));
		Long id = registration.manifest().id();

		BatchFileManifest validating = manifestService.transitionToValidating(id);
		assertThat(validating.status()).isEqualTo(ManifestStatus.VALIDATING);

		BatchFileManifest ready = manifestService.transitionToReady(id);
		assertThat(ready.status()).isEqualTo(ManifestStatus.READY);

		BatchFileManifest processing = manifestService.transitionToProcessing(id);
		assertThat(processing.status()).isEqualTo(ManifestStatus.PROCESSING);
		assertThat(processing.processingStartedAt()).isEqualTo(FIXED_NOW);

		BatchFileManifest completed = manifestService.transitionToCompleted(id, 10_000_000L);
		assertThat(completed.status()).isEqualTo(ManifestStatus.COMPLETED);
		assertThat(completed.recordCount()).isEqualTo(10_000_000L);
		assertThat(completed.processingFinishedAt()).isEqualTo(FIXED_NOW);
	}

	@Test
	void illegalTransitionIsRejected() {
		Registration registration = manifestService.registerDiscovered(MONDAY, "customers.csv",
				URI.create("file:///data/customers.csv"), 42L, "d".repeat(64));
		Long id = registration.manifest().id();

		// SKIP VALIDATING -> direct PROCESSING transition is illegal.
		assertThatThrownBy(() -> manifestService.transitionToProcessing(id))
			.isInstanceOf(BatchInvariantException.class);
	}

	@Test
	void failureCanEscapeValidationOrProcessing() {
		Registration registration = manifestService.registerDiscovered(MONDAY, "customers.csv",
				URI.create("file:///data/customers.csv"), 42L, "e".repeat(64));
		Long id = registration.manifest().id();
		manifestService.transitionToValidating(id);

		BatchFileManifest failed = manifestService.transitionToFailed(id);
		assertThat(failed.status()).isEqualTo(ManifestStatus.FAILED);
	}

	@Test
	void manifestsAreQueryableByBusinessDate() {
		manifestService.registerDiscovered(MONDAY, "customers.csv", URI.create("file:///data/customers.csv"), 42L,
				"f".repeat(64));
		List<BatchFileManifest> manifests = manifestService.findForBusinessDate(MONDAY);
		assertThat(manifests).isNotEmpty().allMatch(m -> m.businessDate().equals(MONDAY));
	}

}
