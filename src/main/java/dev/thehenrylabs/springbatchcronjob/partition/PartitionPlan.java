package dev.thehenrylabs.springbatchcronjob.partition;

/**
 * Target count is deliberately decoupled from current pod count to preserve work
 * stealing.
 */
public record PartitionPlan(int targetPartitions) {
	public PartitionPlan {
		if (targetPartitions < 1)
			throw new IllegalArgumentException("targetPartitions must be positive");
	}
}
