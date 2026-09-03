package dev.thehenrylabs.springbatchcronjob.configuration;

import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Dedicated, bounded batch executor (SPEC 28, 29). Bounded queue + CallerRunsPolicy =
 * backpressure: if workers cannot keep up, the chunk producer eventually runs the work in
 * its own thread instead of accumulating tasks (SPEC 30). Never shared with Quartz, web,
 * or unrelated @Async work.
 */
@Configuration(proxyBeanMethods = false)
public class ExecutorConfiguration {

	public static final String BATCH_PROCESSING_EXECUTOR = "batchProcessingExecutor";

	@Bean(name = BATCH_PROCESSING_EXECUTOR)
	ThreadPoolTaskExecutor batchProcessingExecutor(BatchProperties properties) {
		BatchProperties.Processing.Executor config = properties.processing().executor();
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(config.corePoolSize());
		executor.setMaxPoolSize(config.maxPoolSize());
		executor.setQueueCapacity(config.queueCapacity());
		executor.setThreadNamePrefix("batch-worker-");
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		return executor;
	}

	/**
	 * Executor saturation gauges (SPEC 45): active workers and queue depth make
	 * backpressure observable to operators.
	 */
	@Bean
	java.util.List<io.micrometer.core.instrument.Gauge> batchExecutorGauges(
			io.micrometer.core.instrument.MeterRegistry meterRegistry,
			@org.springframework.beans.factory.annotation.Qualifier(BATCH_PROCESSING_EXECUTOR) ThreadPoolTaskExecutor executor) {
		ThreadPoolExecutor pool = executor.getThreadPoolExecutor();
		return java.util.List.of(
				io.micrometer.core.instrument.Gauge
					.builder("batch_executor_active_threads", pool, ThreadPoolExecutor::getActiveCount)
					.register(meterRegistry),
				io.micrometer.core.instrument.Gauge
					.builder("batch_executor_queue_depth", pool, worker -> worker.getQueue().size())
					.register(meterRegistry));
	}

}
