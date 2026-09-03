package dev.thehenrylabs.springbatchcronjob.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.quartz.CronTrigger;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;

/**
 * Pure unit tests for the weekly trigger contract (FR-001, FR-005): cron expression,
 * timezone, misfire instruction.
 */
class WeeklyCsvScheduleTests {

	private static final ZoneId SGT = ZoneId.of("Asia/Ho_Chi_Minh");

	private static final BatchProperties.MisfirePolicy FIRE_ONCE = BatchProperties.MisfirePolicy.FIRE_ONCE_NOW;

	@Test
	void triggerKeepsCronExpressionAndTimezone() {
		CronTrigger trigger = WeeklyCsvScheduleFactory.buildTrigger("0 0 6 ? * MON", SGT, FIRE_ONCE);
		assertThat(trigger.getCronExpression()).isEqualTo("0 0 6 ? * MON");
		assertThat(trigger.getTimeZone().toZoneId()).isEqualTo(SGT);
	}

	@Test
	void defaultMisfirePolicyIsFireOnceNow() {
		CronTrigger trigger = WeeklyCsvScheduleFactory.buildTrigger("0 0 6 ? * MON", SGT, FIRE_ONCE);
		assertThat(trigger.getMisfireInstruction()).isEqualTo(CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW);
	}

	@Test
	void doNothingMisfirePolicyIsConfigurable() {
		CronTrigger trigger = WeeklyCsvScheduleFactory.buildTrigger("0 0 6 ? * MON", SGT,
				BatchProperties.MisfirePolicy.DO_NOTHING);
		assertThat(trigger.getMisfireInstruction()).isEqualTo(CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING);
	}

	@ParameterizedTest
	@CsvSource({ "2026-09-07T06:00:00Z,2026-09-07", "2026-09-06T23:30:00Z,2026-09-07" })
	void businessDateComesFromScheduledFireTime(String instant, String expected) {
		Date scheduled = Date.from(java.time.Instant.parse(instant));
		assertThat(WeeklyCsvQuartzJob.businessDate(scheduled, SGT)).isEqualTo(java.time.LocalDate.parse(expected));
	}

	@Test
	void businessDateFallsBackToNowWhenScheduledMissing() {
		LocalDate today = LocalDate.now(SGT);
		assertThat(WeeklyCsvQuartzJob.businessDate((Date) null, SGT)).isEqualTo(today);
	}

}
