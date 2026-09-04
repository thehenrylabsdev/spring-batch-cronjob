package dev.thehenrylabs.springbatchcronjob.domain;

/** Lifecycle advertised by a running application instance. */
public enum WorkerStatus {

	STARTING, IDLE, BUSY, DRAINING, STOPPED

}
