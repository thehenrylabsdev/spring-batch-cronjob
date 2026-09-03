package dev.thehenrylabs.springbatchcronjob.job.processing;

import java.time.LocalDate;
import java.util.Set;

import org.springframework.stereotype.Component;

/**
 * Immutable per-run processing context published by {@code loadReferenceDataStep} (SPEC
 * 20): business date and the preloaded reference customer set. The classifier reads a
 * single immutable snapshot per run — no per-record remote lookups (SPEC 25).
 *
 * <p>
 * The swap is atomic, so readers are safe under concurrent processing; the caller
 * validates that a context exists before the processing step starts.
 */
@Component
public class ProcessingContext {

	private volatile Snapshot snapshot;

	public static final record Snapshot(LocalDate businessDate, Set<String> eligibleCustomerIds) {

		public Snapshot {
			eligibleCustomerIds = Set.copyOf(eligibleCustomerIds);
		}
	}

	public void publish(LocalDate businessDate, Set<String> eligibleCustomerIds) {
		this.snapshot = new Snapshot(businessDate, eligibleCustomerIds);
	}

	public Snapshot current() {
		Snapshot snapshot = this.snapshot;
		if (snapshot == null) {
			throw new IllegalStateException("ProcessingContext was not published — loadReferenceDataStep missing");
		}
		return snapshot;
	}

}
