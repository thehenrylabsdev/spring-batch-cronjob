-- Preserve SKIPPED as a first-class outcome for distributed reconciliation.
SET search_path TO control;

ALTER TABLE batch_partition ADD COLUMN IF NOT EXISTS skipped_count BIGINT NOT NULL DEFAULT 0;
