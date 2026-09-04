package dev.thehenrylabs.springbatchcronjob.scheduler;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import dev.thehenrylabs.springbatchcronjob.service.BatchLaunchService;

/**
 * Quartz entry point (FR-002): resolves the trigger occurrence, derives the business date
 * from the scheduled fire time, launches the Spring Batch job, then returns control.
 *
 * <p>
 * Quartz NEVER processes CSV records itself — it only decides *when* work starts.
 *
 * <p>
 * The scheduler context carries the {@link BatchLaunchService} delegate: the job class
 * must have a no-arg constructor (Quartz instantiation), and Spring's job factory does
 * not guarantee constructor injection. The scheduling factory (running in the same JVM as
 * the firing job) publishes the delegate before any trigger can fire.
 */
@DisallowConcurrentExecution
public class WeeklyCsvQuartzJob implements Job {

	/** Job key used in the durable Quartz JobStore. */
	public static final String JOB_NAME = "weekly-csv-coordinator";

	public static final String JOB_GROUP = "weekly-batch";

	/** Scheduler context key for the launch delegate. */
	public static final String LAUNCHER_KEY = WeeklyCsvQuartzJob.class.getName() + ".launcher";

	/** Scheduler context key for the configured timezone (SPEC FR-001). */
	public static final String ZONE_KEY = WeeklyCsvQuartzJob.class.getName() + ".zone";

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		BatchLaunchService launchService = launcher(context);
		ZoneId zone = zone(context);
		LocalDate businessDate = businessDate(context, zone);
		launchService.launchWeekly(businessDate);
	}

	/**
	 * Business date is derived from the trigger's scheduled fire time: a misfire firing
	 * on Tuesday still owns Monday's identity (SPEC FR-005, 11).
	 */
	static LocalDate businessDate(JobExecutionContext context, ZoneId zone) {
		return businessDate(context.getScheduledFireTime(), zone);
	}

	static LocalDate businessDate(java.util.Date scheduled, ZoneId zone) {
		Instant fireTime = scheduled != null ? scheduled.toInstant() : Instant.now();
		return fireTime.atZone(zone).toLocalDate();
	}

	private static <T> T schedulerContext(JobExecutionContext context, String key) throws JobExecutionException {
		try {
			@SuppressWarnings("unchecked")
			T value = (T) context.getScheduler().getContext().get(key);
			return value;
		}
		catch (org.quartz.SchedulerException e) {
			throw new JobExecutionException("Cannot read scheduler context", e);
		}
	}

	private static BatchLaunchService launcher(JobExecutionContext context) throws JobExecutionException {
		BatchLaunchService service = schedulerContext(context, LAUNCHER_KEY);
		if (service == null) {
			throw new JobExecutionException("Batch launch delegate not published in scheduler context");
		}
		return service;
	}

	private static ZoneId zone(JobExecutionContext context) throws JobExecutionException {
		String name = schedulerContext(context, ZONE_KEY);
		if (name == null) {
			throw new JobExecutionException("Zone not published in scheduler context");
		}
		return ZoneId.of(name);
	}

}
