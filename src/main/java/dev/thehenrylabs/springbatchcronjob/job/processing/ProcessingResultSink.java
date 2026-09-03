package dev.thehenrylabs.springbatchcronjob.job.processing;

import org.springframework.batch.infrastructure.item.Chunk;

import dev.thehenrylabs.springbatchcronjob.domain.ProcessingResult;

/**
 * Downstream-neutral result sink (SPEC 26, 75). This version ships the lightweight
 * metrics writer; a future message-broker writer plugs in here without touching the
 * reader, processor, or checkpoint model. No broker-specific type may appear in the
 * domain-processing APIs.
 */
public interface ProcessingResultSink {

	void write(Chunk<? extends ProcessingResult> results);

}
