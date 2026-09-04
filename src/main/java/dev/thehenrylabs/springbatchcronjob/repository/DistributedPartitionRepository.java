package dev.thehenrylabs.springbatchcronjob.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import dev.thehenrylabs.springbatchcronjob.domain.BatchPartition;
import dev.thehenrylabs.springbatchcronjob.domain.PartitionStatus;

/** PostgreSQL work queue. Every ownership-changing operation is short and fenced. */
@Repository
public class DistributedPartitionRepository {

	private final NamedParameterJdbcTemplate jdbc;

	public DistributedPartitionRepository(NamedParameterJdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	public Optional<BatchPartition> claim(long executionId, String workerId, Instant expiresAt, Instant now,
			int maxAttempts) {
		List<BatchPartition> rows = this.jdbc.query(
				"""
						WITH candidate AS (
						 SELECT id FROM control.batch_partition WHERE job_execution_id=:execution
						 AND status IN ('READY','RETRYABLE') AND attempt_count < :maxAttempts
						 ORDER BY partition_no FOR UPDATE SKIP LOCKED LIMIT 1
						) UPDATE control.batch_partition p SET status='CLAIMED',worker_id=:worker,lease_version=p.lease_version+1,
						lease_expires_at=:expires,claimed_at=:now,attempt_count=p.attempt_count+1,updated_at=:now
						FROM candidate WHERE p.id=candidate.id RETURNING p.*
						""",
				new MapSqlParameterSource().addValue("execution", executionId)
					.addValue("worker", workerId)
					.addValue("expires", timestamp(expiresAt))
					.addValue("now", timestamp(now))
					.addValue("maxAttempts", maxAttempts),
				this::map);
		return rows.stream().findFirst();
	}

	public Optional<BatchPartition> claimAny(String workerId, Instant expiresAt, Instant now, int maxAttempts) {
		List<BatchPartition> rows = this.jdbc.query(
				"""
						WITH candidate AS (
						 SELECT id FROM control.batch_partition WHERE status IN ('READY','RETRYABLE') AND attempt_count < :maxAttempts
						 ORDER BY job_execution_id,partition_no FOR UPDATE SKIP LOCKED LIMIT 1
						) UPDATE control.batch_partition p SET status='CLAIMED',worker_id=:worker,lease_version=p.lease_version+1,
						lease_expires_at=:expires,claimed_at=:now,attempt_count=p.attempt_count+1,updated_at=:now
						FROM candidate WHERE p.id=candidate.id RETURNING p.*
						""",
				new MapSqlParameterSource().addValue("worker", workerId)
					.addValue("expires", timestamp(expiresAt))
					.addValue("now", timestamp(now))
					.addValue("maxAttempts", maxAttempts),
				this::map);
		return rows.stream().findFirst();
	}

	public int start(long id, String worker, long version, Instant expires, Instant now) {
		return fenced(
				"UPDATE control.batch_partition SET status='RUNNING',lease_expires_at=:expires,updated_at=:now WHERE "
						+ where(),
				id, worker, version, now, expires);
	}

	public int renew(long id, String worker, long version, Instant expires, Instant now) {
		return fenced("UPDATE control.batch_partition SET lease_expires_at=:expires,updated_at=:now WHERE " + where(),
				id, worker, version, now, expires);
	}

	public int checkpoint(long id, String worker, long version, long byteOffset, long recordNo, long read,
			long processed, long accepted, long rejected, long invalid, long skipped, Instant now) {
		return this.jdbc.update(
				"""
						UPDATE control.batch_partition SET checkpoint_byte=:byte,checkpoint_record_no=:record,committed_read_count=:read,
						committed_processed_count=:processed,accepted_count=:accepted,rejected_count=:rejected,invalid_count=:invalid,
						skipped_count=:skipped,updated_at=:now WHERE id=:id AND worker_id=:worker AND lease_version=:version AND status='RUNNING'
						""",
				fencedArgs(id, worker, version, now).addValue("byte", byteOffset)
					.addValue("record", recordNo)
					.addValue("read", read)
					.addValue("processed", processed)
					.addValue("accepted", accepted)
					.addValue("rejected", rejected)
					.addValue("invalid", invalid)
					.addValue("skipped", skipped));
	}

	public int complete(long id, String worker, long version, Instant now) {
		return fenced(
				"UPDATE control.batch_partition SET status='COMPLETED',completed_at=:now,lease_expires_at=NULL,updated_at=:now WHERE "
						+ runningWhere(),
				id, worker, version, now, null);
	}

	public int release(long id, String worker, long version, Instant now) {
		return fenced(
				"UPDATE control.batch_partition SET status='RETRYABLE',worker_id=NULL,lease_expires_at=NULL,updated_at=:now WHERE "
						+ where(),
				id, worker, version, now, null);
	}

	public int reapExpired(Instant now) {
		return this.jdbc.update(
				"""
						UPDATE control.batch_partition SET status='RETRYABLE',worker_id=NULL,lease_expires_at=NULL,updated_at=:now
						WHERE status IN ('CLAIMED','RUNNING') AND lease_expires_at < :now
						""",
				java.util.Map.of("now", timestamp(now)));
	}

	public List<BatchPartition> forExecution(long executionId) {
		return this.jdbc.query(
				"SELECT * FROM control.batch_partition WHERE job_execution_id=:execution ORDER BY partition_no",
				java.util.Map.of("execution", executionId), this::map);
	}

	/**
	 * Partition definitions belong to the file manifest and survive coordinator
	 * executions.
	 */
	public List<BatchPartition> forManifest(long manifestId) {
		return this.jdbc.query(
				"SELECT * FROM control.batch_partition WHERE manifest_id=:manifest ORDER BY partition_no",
				java.util.Map.of("manifest", manifestId), this::map);
	}

	public void create(long manifestId, long executionId,
			List<dev.thehenrylabs.springbatchcronjob.partition.CsvPartitionDefinition> definitions, Instant now) {
		for (dev.thehenrylabs.springbatchcronjob.partition.CsvPartitionDefinition definition : definitions) {
			this.jdbc.update(
					"""
							INSERT INTO control.batch_partition (manifest_id,job_execution_id,partition_no,start_byte,end_byte_exclusive,
							start_record_no,end_record_no_exclusive,expected_record_count,status,checkpoint_byte,checkpoint_record_no,created_at,updated_at)
							VALUES (:manifest,:execution,:number,:startByte,:endByte,:startRecord,:endRecord,:expected,'READY',:startByte,:startRecord,:now,:now)
							ON CONFLICT (manifest_id,partition_no) DO NOTHING
							""",
					new MapSqlParameterSource().addValue("manifest", manifestId)
						.addValue("execution", executionId)
						.addValue("number", definition.partitionNo())
						.addValue("startByte", definition.startByte())
						.addValue("endByte", definition.endByteExclusive())
						.addValue("startRecord", definition.startRecordNo())
						.addValue("endRecord", definition.endRecordNoExclusive())
						.addValue("expected", definition.expectedRecordCount())
						.addValue("now", timestamp(now)));
		}
	}

	private int fenced(String sql, long id, String worker, long version, Instant now, Instant expires) {
		MapSqlParameterSource args = fencedArgs(id, worker, version, now);
		if (expires != null)
			args.addValue("expires", timestamp(expires));
		return this.jdbc.update(sql, args);
	}

	private static String where() {
		return "id=:id AND worker_id=:worker AND lease_version=:version AND status IN ('CLAIMED','RUNNING')";
	}

	private static String runningWhere() {
		return "id=:id AND worker_id=:worker AND lease_version=:version AND status='RUNNING'";
	}

	private static MapSqlParameterSource fencedArgs(long id, String worker, long version, Instant now) {
		return new MapSqlParameterSource().addValue("id", id)
			.addValue("worker", worker)
			.addValue("version", version)
			.addValue("now", timestamp(now));
	}

	private static java.sql.Timestamp timestamp(Instant instant) {
		return java.sql.Timestamp.from(instant);
	}

	private BatchPartition map(ResultSet rs, int ignored) throws SQLException {
		return new BatchPartition(rs.getLong("id"), rs.getLong("manifest_id"), rs.getLong("job_execution_id"),
				(Long) rs.getObject("step_execution_id"), rs.getString("step_name"), rs.getInt("partition_no"),
				rs.getLong("start_byte"), rs.getLong("end_byte_exclusive"), rs.getLong("start_record_no"),
				rs.getLong("end_record_no_exclusive"), rs.getLong("expected_record_count"),
				PartitionStatus.valueOf(rs.getString("status")), rs.getString("worker_id"), rs.getLong("lease_version"),
				rs.getTimestamp("lease_expires_at") == null ? null : rs.getTimestamp("lease_expires_at").toInstant(),
				rs.getLong("checkpoint_byte"), rs.getLong("checkpoint_record_no"), rs.getLong("committed_read_count"),
				rs.getLong("committed_processed_count"), rs.getLong("accepted_count"), rs.getLong("rejected_count"),
				rs.getLong("invalid_count"), rs.getLong("skipped_count"), rs.getInt("attempt_count"));
	}

}
