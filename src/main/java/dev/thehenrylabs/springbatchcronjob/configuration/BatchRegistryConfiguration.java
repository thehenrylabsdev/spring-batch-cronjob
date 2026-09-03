package dev.thehenrylabs.springbatchcronjob.configuration;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Application-visible {@link JobRegistry} used by the launch service (and by job
 * definitions that register themselves). Boot's batch configuration provides the
 * repository and operator, but not a registry bean — the registry is explicitly owned
 * here so registrations are unambiguous across the two replicas.
 */
@Configuration(proxyBeanMethods = false)
public class BatchRegistryConfiguration {

	@Bean
	JobRegistry jobRegistry() {
		return new MapJobRegistry();
	}

}
