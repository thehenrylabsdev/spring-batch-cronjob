package dev.thehenrylabs.springbatchcronjob.configuration;

import java.time.Duration;

import org.springframework.core.retry.RetryPolicy;
import org.springframework.dao.DataAccessException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Exception-specific bounded retry policy (SPEC 34.3, 35): only transient
 * {@link DataAccessException}s are retried — deterministic parsing errors never are, and
 * the overall attempt budget is capped by the step's retryLimit. Backoff follows the
 * configured exponential policy.
 */
public class TransientDataAccessRetryPolicy implements RetryPolicy {

	private final BackOff backOff;

	public TransientDataAccessRetryPolicy(Duration initialBackoff, double multiplier, Duration maxBackoff) {
		ExponentialBackOff exponential = new ExponentialBackOff(initialBackoff.toNanos() / 1_000_000, multiplier);
		exponential.setMaxInterval(maxBackoff.toNanos() / 1_000_000);
		this.backOff = exponential;
	}

	@Override
	public boolean shouldRetry(Throwable t) {
		return t instanceof DataAccessException;
	}

	@Override
	public Duration getTimeout() {
		return Duration.ofMinutes(1);
	}

	@Override
	public BackOff getBackOff() {
		return this.backOff;
	}

}
