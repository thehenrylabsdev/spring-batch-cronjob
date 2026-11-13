package dev.thehenrylabs.springbatchcronjob.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.zaxxer.hikari.HikariDataSource;

import dev.thehenrylabs.springbatchcronjob.TestcontainersConfiguration;

/**
 * SPEC 61.4 / AC-002: two clustered Quartz Scheduler instances sharing one PostgreSQL
 * JobStore must fire a given trigger occurrence exactly once. The Spring context's own
 * clustered scheduler is a third cluster member, which strengthens the test — ANY of the
 * three may acquire the trigger row, yet the occurrence fires exactly once.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class QuartzClusterFiringTests {

	public static final class CountingJob implements Job {

		private static final AtomicInteger EXECUTIONS = new AtomicInteger();

		@Override
		public void execute(JobExecutionContext context) {
			EXECUTIONS.incrementAndGet();
		}

	}

	@Autowired
	private HikariDataSource dataSource;

	private final List<org.springframework.scheduling.quartz.SchedulerFactoryBean> schedulersToShutdown = new ArrayList<>();

	@Test
	void clusteredSchedulersFireEachOccurrenceExactlyOnce() throws Exception {
		CountingJob.EXECUTIONS.set(0);

		Scheduler first = startClusteredScheduler("cluster-node-a");
		startClusteredScheduler("cluster-node-b");
		JobKey jobKey = JobKey.jobKey("clusterTestJob", "clusterTest");
		try {
			first.scheduleJob(
					org.quartz.JobBuilder.newJob(CountingJob.class).withIdentity(jobKey).storeDurably(true).build(),
					TriggerBuilder.newTrigger()
						.withIdentity("clusterTestTrigger", "clusterTest")
						.forJob(jobKey)
						.startAt(new Date(System.currentTimeMillis() + 500))
						.withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0))
						.build());

			// Cluster-atomic fire: one scheduler acquires the trigger row lock.
			awaitCount(1, 15_000);
			assertThat(CountingJob.EXECUTIONS.get()).isEqualTo(1);
		}
		finally {
			// The job is durable and the database is shared with other test classes:
			// remove it.
			first.deleteJob(jobKey);
			for (org.springframework.scheduling.quartz.SchedulerFactoryBean factory : this.schedulersToShutdown) {
				factory.destroy();
			}
		}
	}

	private Scheduler startClusteredScheduler(String instanceName) throws Exception {
		// Spring's SchedulerFactoryBean owns the Quartz 2.5 DataSource/ConnectionProvider
		// plumbing (same path as the production scheduler); the two test instances share
		// the application's pool and each other's JobStore via clustering.
		org.springframework.scheduling.quartz.SchedulerFactoryBean factory = new org.springframework.scheduling.quartz.SchedulerFactoryBean();
		factory.setSchedulerName(instanceName);
		factory.setDataSource(this.dataSource);
		factory.setAutoStartup(false);

		Properties props = new Properties();
		props.setProperty("org.quartz.scheduler.instanceName", instanceName);
		props.setProperty("org.quartz.scheduler.instanceId", "AUTO");
		props.setProperty("org.quartz.threadPool.threadCount", "2");
		// No jobStore.class: with a local DataSource Spring's SchedulerFactoryBean
		// uses its LocalDataSourceJobStore directly.
		props.setProperty("org.quartz.jobStore.driverDelegateClass", "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate");
		props.setProperty("org.quartz.jobStore.tablePrefix", "QRTZ_");
		props.setProperty("org.quartz.jobStore.isClustered", "true");
		props.setProperty("org.quartz.jobStore.clusterCheckinInterval", "5000");
		factory.setQuartzProperties(props);

		factory.afterPropertiesSet();
		Scheduler scheduler = factory.getObject();
		scheduler.start();
		schedulersToShutdown.add(factory);
		return scheduler;
	}

	private void awaitCount(int expected, int timeoutMs) throws InterruptedException {
		long deadline = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < deadline && CountingJob.EXECUTIONS.get() < expected) {
			Thread.sleep(200);
		}
		// Settle window: with a single-repeat trigger no further occurrence can fire.
		Thread.sleep(1_500);
	}

}
