package dev.thehenrylabs.springbatchcronjob.scheduler;

import java.time.ZoneId;
import java.util.TimeZone;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.quartz.spi.MutableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.configuration.BatchProperties;
import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * Bootstrap for the durable weekly trigger (FR-001..FR-005).
 *
 * <p>
 * Each application instance performs an idempotent, cluster-safe registration: if the
 * job/trigger keys already exist in the JDBC JobStore, nothing is changed. Concurrent
 * registration from two replicas is serialized by the Quartz trigger-access lock; the
 * losing {@link ObjectAlreadyExistsException} is treated as success.
 *
 * <p>
 * This runner also publishes the launch delegate and timezone into the scheduler context
 * of the local instance — the firing job always runs on this instance, so the values are
 * available before any trigger can fire.
 */
@Component
public class WeeklyCsvScheduleFactory implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(WeeklyCsvScheduleFactory.class);

	private final Scheduler scheduler;

	private final BatchProperties properties;

	private final BatchLaunchService launchService;

	private final dev.thehenrylabs.springbatchcronjob.observability.BatchMetrics metrics;

	public WeeklyCsvScheduleFactory(Scheduler scheduler, BatchProperties properties, BatchLaunchService launchService,
			dev.thehenrylabs.springbatchcronjob.observability.BatchMetrics metrics) {
		this.scheduler = scheduler;
		this.properties = properties;
		this.launchService = launchService;
		this.metrics = metrics;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		if (!this.properties.schedule().enabled()) {
			log.info("Weekly CSV schedule is disabled; no Quartz trigger registered");
			return;
		}
		ZoneId zone = ZoneId.of(this.properties.schedule().zone());
		this.scheduler.getContext().put(WeeklyCsvQuartzJob.LAUNCHER_KEY, this.launchService);
		this.scheduler.getContext().put(WeeklyCsvQuartzJob.ZONE_KEY, zone.getId());
		this.scheduler.getListenerManager().addTriggerListener(triggerMetrics());
		registerIfAbsent(zone);
	}

	private void registerIfAbsent(ZoneId zone) throws Exception {
		JobKey jobKey = JobKey.jobKey(WeeklyCsvQuartzJob.JOB_NAME, WeeklyCsvQuartzJob.JOB_GROUP);
		TriggerKey triggerKey = triggerKey();
		if (this.scheduler.checkExists(jobKey) && this.scheduler.checkExists(triggerKey)) {
			log.info("Weekly CSV trigger already registered: {}", triggerKey);
			return;
		}
		try {
			this.scheduler.scheduleJob(jobDetail(),
					buildTrigger(this.properties.schedule().cron(), zone, this.properties.schedule().misfirePolicy()));
			log.info("Registered weekly CSV trigger cron={} zone={}", this.properties.schedule().cron(),
					this.properties.schedule().zone());
		}
		catch (ObjectAlreadyExistsException already) {
			// Another replica registered first; the JDBC JobStore serialized the race.
			log.info("Weekly CSV trigger already registered by another scheduler instance: {}", triggerKey);
		}
	}

	JobDetail jobDetail() {
		return JobBuilder.newJob(WeeklyCsvQuartzJob.class)
			.withIdentity(WeeklyCsvQuartzJob.JOB_NAME, WeeklyCsvQuartzJob.JOB_GROUP)
			.storeDurably(true)
			.requestRecovery(true)
			.build();
	}

	static TriggerKey triggerKey() {
		return TriggerKey.triggerKey(WeeklyCsvQuartzJob.JOB_NAME, WeeklyCsvQuartzJob.JOB_GROUP);
	}

	private org.quartz.listeners.TriggerListenerSupport triggerMetrics() {
		return new org.quartz.listeners.TriggerListenerSupport() {
			@Override
			public String getName() {
				return "batchMetrics";
			}

			@Override
			public void triggerFired(org.quartz.Trigger trigger, org.quartz.JobExecutionContext context) {
				WeeklyCsvScheduleFactory.this.metrics.quartzTriggerFired();
			}

			@Override
			public void triggerMisfired(org.quartz.Trigger trigger) {
				WeeklyCsvScheduleFactory.this.metrics.quartzTriggerMisfire();
			}
		};
	}

	/**
	 * Builds the weekly {@link CronTrigger}. Misfire mapping (FR-005): FIRE_ONCE_NOW ->
	 * {@link CronTrigger#MISFIRE_INSTRUCTION_FIRE_ONCE_NOW} (via FireAndProceed),
	 * DO_NOTHING -> {@link CronTrigger#MISFIRE_INSTRUCTION_DO_NOTHING}.
	 */
	static CronTrigger buildTrigger(String cron, ZoneId zone, BatchProperties.MisfirePolicy misfirePolicy) {
		CronScheduleBuilder schedule = CronScheduleBuilder.cronSchedule(cron).inTimeZone(TimeZone.getTimeZone(zone));
		switch (misfirePolicy) {
			case FIRE_ONCE_NOW -> schedule.withMisfireHandlingInstructionFireAndProceed();
			case DO_NOTHING -> schedule.withMisfireHandlingInstructionDoNothing();
		}
		// Quartz 2.5 widens build() to MutableTrigger; the underlying type is
		// CronTrigger. Identity must be assigned: scheduleJob rejects unnamed triggers.
		MutableTrigger trigger = schedule.build();
		trigger.setKey(triggerKey());
		trigger.setJobKey(JobKey.jobKey(WeeklyCsvQuartzJob.JOB_NAME, WeeklyCsvQuartzJob.JOB_GROUP));
		return (CronTrigger) trigger;
	}

}
