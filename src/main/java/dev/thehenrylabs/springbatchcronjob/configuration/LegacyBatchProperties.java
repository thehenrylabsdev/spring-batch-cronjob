package dev.thehenrylabs.springbatchcronjob.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Explicit cutover guard: legacy and distributed schedulers must never run together. */
@ConfigurationProperties(prefix = "batch.legacy")
public record LegacyBatchProperties(boolean enabled) {
}
