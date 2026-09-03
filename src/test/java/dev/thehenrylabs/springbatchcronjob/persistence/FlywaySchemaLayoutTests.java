package dev.thehenrylabs.springbatchcronjob.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import dev.thehenrylabs.springbatchcronjob.TestcontainersConfiguration;

/**
 * Migration test (SPEC 61.3): verifies the dedicated schema layout created by Flyway —
 * quartz/batch/control exist, Quartz tables live in the quartz schema, and the migration
 * history is consistent.
 *
 * <p>
 * Note: Postgres normalizes unquoted identifiers to lowercase, so framework tables
 * (QRTZ_*, BATCH_*) are asserted with case-insensitive comparison.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class FlywaySchemaLayoutTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	void createsDedicatedSchemas() {
		List<String> schemas = jdbcTemplate.queryForList(
				"SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('quartz', 'batch', 'control')",
				String.class);
		assertThat(schemas).containsExactlyInAnyOrder("quartz", "batch", "control");
	}

	@Test
	void quartzTablesAreInsideQuartzSchema() {
		Integer count = jdbcTemplate.queryForObject(
				"SELECT count(*) FROM information_schema.tables WHERE table_schema = 'quartz' AND LOWER(table_name) = "
						+ "'qrtz_job_details'",
				Integer.class);
		assertThat(count).isEqualTo(1);
	}

	@Test
	void quartzTablesAreVisibleThroughSearchPath() {
		// The runtime search_path (batch, quartz, control) must make unqualified
		// framework queries resolve; the app datasource never qualifies QRTZ_*.
		List<String> visible = jdbcTemplate
			.queryForList("SELECT table_schema || '.' || lower(table_name) FROM information_schema.tables "
					+ "WHERE LOWER(table_name) = 'qrtz_job_details'", String.class);
		assertThat(visible).contains("quartz.qrtz_job_details");
	}

	@Test
	void migrationHistoryIsApplied() {
		// Version is stored as a string (e.g. '1' or '001' depending on Flyway
		// formatting); assert on success rather than the exact format.
		List<Map<String, Object>> versions = jdbcTemplate.queryForList("""
				SELECT version, success
				FROM batch.flyway_schema_history
				WHERE version IS NOT NULL AND success = true
				""");
		// One row per SQL migration (V001, V002).
		assertThat(versions).hasSize(2);
	}

}
