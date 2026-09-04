package dev.thehenrylabs.springbatchcronjob.domain;

/** Durable state of one safe CSV byte range. */
public enum PartitionStatus {

	READY, CLAIMED, RUNNING, COMPLETED, FAILED, RETRYABLE, CANCELLED

}
