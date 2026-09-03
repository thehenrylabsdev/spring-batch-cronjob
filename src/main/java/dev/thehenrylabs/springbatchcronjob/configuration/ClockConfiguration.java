package dev.thehenrylabs.springbatchcronjob.configuration;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Shared clock bean so domain events can be time-zone-independent and deterministic in
 * tests (fixed clock override).
 */
@Configuration(proxyBeanMethods = false)
public class ClockConfiguration {

	@Bean
	Clock clock() {
		return Clock.systemUTC();
	}

}
