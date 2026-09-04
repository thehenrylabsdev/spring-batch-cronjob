package dev.thehenrylabs.springbatchcronjob.domain;

import java.time.Instant;
import java.time.LocalDate;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

/**
 * Application-owned manifest row, complementing the Spring Batch {@code JobRepository}
 * (SPEC 12). Deterministic identity is {@code (businessDate, checksumSha256)}.
 *
 * @param id database identity
 * @param businessDate weekly run date (Monday)
 * @param fileName file name as discovered
 * @param fileUri canonical source URI
 * @param fileSizeBytes validated file size
 * @param checksumSha256 SHA-256 hex digest (uncompressed)
 * @param status lifecycle state
 * @param discoveredAt when the file was first seen
 * @param processingStartedAt when {@code processCsvStep} began
 * @param processingFinishedAt when the job terminal state was reached
 * @param recordCount total CSV records processed
 * @param createdAt row creation time
 * @param updatedAt last modification time
 */
@Table(name = "batch_file_manifest", schema = "control")
public record BatchFileManifest(@Id Long id, LocalDate businessDate, String fileName, String fileUri,
		long fileSizeBytes, String checksumSha256, ManifestStatus status, Instant discoveredAt,
		Instant processingStartedAt, Instant processingFinishedAt, Long recordCount, Instant createdAt,
		Instant updatedAt, String logicalName, String fileGeneration, String headerHash, Long expectedRecordCount,
		Instant indexStartedAt, Instant indexFinishedAt, Integer activeWorkerCountAtPlan,
		Integer activeWorkerSlotsAtPlan) {

}
