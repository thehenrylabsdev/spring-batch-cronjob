package dev.thehenrylabs.springbatchcronjob.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.quartz.CronTrigger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import dev.thehenrylabs.springbatchcronjob.TestcontainersConfiguration;

/**
 * FR-003/FR-004: the weekly trigger is persisted in the JDBC JobStore, survives context
 * restarts, and registration is idempotent across cluster nodes.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class WeeklyCsvScheduleFactoryTests {

	@Autowired
	private Scheduler scheduler;

	@Autowired
	private WeeklyCsvScheduleFactory factory;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	void triggerIsRegisteredInJdbcStore() throws Exception {
		JobKey jobKey = JobKey.jobKey(WeeklyCsvQuartzJob.JOB_NAME, WeeklyCsvQuartzJob.JOB_GROUP);
		assertThat(this.scheduler.checkExists(jobKey)).isTrue();
		Trigger trigger = this.scheduler.getTrigger(WeeklyCsvScheduleFactory.triggerKey());
		assertThat(trigger).isInstanceOf(CronTrigger.class);

		Integer jobs = this.jdbcTemplate.queryForObject(
				"SELECT count(*) FROM quartz.qrtz_job_details WHERE job_group = ?", Integer.class,
				WeeklyCsvQuartzJob.JOB_GROUP);
		assertThat(jobs).isEqualTo(1);
	}

	@Test
	void triggerCarriesConfiguredCronAndZone() throws Exception {
		Trigger trigger = this.scheduler.getTrigger(WeeklyCsvScheduleFactory.triggerKey());
		CronTrigger cron = (CronTrigger) trigger;
		assertThat(cron.getCronExpression()).isEqualTo("0 0 6 ? * MON");
		assertThat(cron.getTimeZone().toZoneId().getId()).isEqualTo("Asia/Ho_Chi_Minh");
	}

	@Test
	void registrationIsIdempotent() throws Exception {
		// ApplicationRunner already ran at startup; a second run must not duplicate rows.
		// Counts are scoped to this job's group: other test classes share the database
		// and store their own jobs.
		this.factory.run(null);
		Integer jobs = this.jdbcTemplate.queryForObject(
				"SELECT count(*) FROM quartz.qrtz_job_details WHERE job_group = ?", Integer.class,
				WeeklyCsvQuartzJob.JOB_GROUP);
		Integer triggers = this.jdbcTemplate.queryForObject(
				"SELECT count(*) FROM quartz.qrtz_triggers WHERE job_group = ?", Integer.class,
				WeeklyCsvQuartzJob.JOB_GROUP);
		assertThat(jobs).isEqualTo(1);
		assertThat(triggers).isEqualTo(1);
	}

}
