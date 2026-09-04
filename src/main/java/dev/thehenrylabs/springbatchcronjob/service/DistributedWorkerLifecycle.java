package dev.thehenrylabs.springbatchcronjob.service;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.context.SmartLifecycle;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import dev.thehenrylabs.springbatchcronjob.configuration.DistributedBatchProperties;
import dev.thehenrylabs.springbatchcronjob.configuration.WorkerIdentity;
import dev.thehenrylabs.springbatchcronjob.domain.BatchWorker;
import dev.thehenrylabs.springbatchcronjob.domain.WorkerStatus;
import dev.thehenrylabs.springbatchcronjob.repository.DistributedWorkerRepository;

/**
 * Registers once and keeps PostgreSQL worker liveness authoritative throughout shutdown.
 */
@Component
public class DistributedWorkerLifecycle implements SmartLifecycle {

	private final DistributedWorkerRepository workers;

	private final WorkerIdentity identity;

	private final DistributedBatchProperties properties;

	private final Clock clock;

	private final String version;

	private ScheduledExecutorService heartbeatExecutor;

	private volatile boolean running;

	private volatile WorkerStatus status = WorkerStatus.STARTING;

	private volatile Long executionId;

	private volatile Long partitionId;

	public DistributedWorkerLifecycle(DistributedWorkerRepository workers, WorkerIdentity identity,
			DistributedBatchProperties properties, Clock clock, Environment environment) {
		this.workers = workers;
		this.identity = identity;
		this.properties = properties;
		this.clock = clock;
		this.version = environment.getProperty("info.app.version", "unknown");
	}

	@Override
	public void start() {
		if (!this.properties.enabled() || this.running)
			return;
		this.running = true;
		this.status = WorkerStatus.IDLE;
		beat();
		this.heartbeatExecutor = Executors
			.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("worker-heartbeat-", 0).factory());
		long interval = this.properties.worker().heartbeatInterval().toMillis();
		this.heartbeatExecutor.scheduleWithFixedDelay(this::safeBeat, interval, interval, TimeUnit.MILLISECONDS);
	}

	public void working(long jobExecution, long partition) {
		this.executionId = jobExecution;
		this.partitionId = partition;
		this.status = WorkerStatus.BUSY;
		beat();
	}

	public void idle() {
		this.executionId = null;
		this.partitionId = null;
		this.status = WorkerStatus.IDLE;
		beat();
	}

	@Override
	public void stop() {
		if (!this.running)
			return;
		this.status = WorkerStatus.DRAINING;
		beat();
		this.running = false;
		if (this.heartbeatExecutor != null)
			this.heartbeatExecutor.shutdownNow();
		this.status = WorkerStatus.STOPPED;
		beat();
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public int getPhase() {
		return Integer.MIN_VALUE;
	}

	private void safeBeat() {
		try {
			beat();
		}
		catch (RuntimeException ignored) {
			/* DB outage: lease expiry provides recovery */ }
	}

	private void beat() {
		Instant now = this.clock.instant();
		BatchWorker worker = new BatchWorker(this.identity.workerId(), this.identity.podName(),
				this.identity.namespace(), this.version, this.status, this.properties.worker().concurrency(), now, now,
				this.executionId, this.partitionId, this.status == WorkerStatus.DRAINING);
		this.workers.register(worker);
		this.workers.heartbeat(this.identity.workerId(), this.status, this.executionId, this.partitionId, now);
	}

}
