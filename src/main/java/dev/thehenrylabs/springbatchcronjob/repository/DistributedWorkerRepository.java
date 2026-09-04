package dev.thehenrylabs.springbatchcronjob.repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import dev.thehenrylabs.springbatchcronjob.domain.BatchWorker;
import dev.thehenrylabs.springbatchcronjob.domain.WorkerStatus;

/** Native SQL repository for worker presence. */
@Repository
public class DistributedWorkerRepository {

	private final NamedParameterJdbcTemplate jdbc;

	public DistributedWorkerRepository(NamedParameterJdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	public void register(BatchWorker worker) {
		this.jdbc.update(
				"""
						INSERT INTO control.batch_worker (worker_id,pod_name,pod_namespace,application_version,status,worker_slots,started_at,last_heartbeat_at,shutdown_requested,created_at,updated_at)
						VALUES (:id,:pod,:ns,:version,:status,:slots,:now,:now,false,:now,:now)
						ON CONFLICT (worker_id) DO UPDATE SET pod_name=EXCLUDED.pod_name,pod_namespace=EXCLUDED.pod_namespace,
						application_version=EXCLUDED.application_version,status=EXCLUDED.status,worker_slots=EXCLUDED.worker_slots,
						last_heartbeat_at=EXCLUDED.last_heartbeat_at,shutdown_requested=false,updated_at=EXCLUDED.updated_at
						""",
				args(worker));
	}

	public int heartbeat(String workerId, WorkerStatus status, Long executionId, Long partitionId, Instant now) {
		return this.jdbc.update(
				"""
						UPDATE control.batch_worker SET status=:status,current_job_execution=:execution,current_partition_id=:partition,
						last_heartbeat_at=:now,updated_at=:now WHERE worker_id=:id
						""",
				new MapSqlParameterSource().addValue("id", workerId)
					.addValue("status", status.name())
					.addValue("execution", executionId)
					.addValue("partition", partitionId)
					.addValue("now", java.sql.Timestamp.from(now)));
	}

	public int activeSlots(Instant cutoff) {
		Integer result = this.jdbc.queryForObject("""
				SELECT COALESCE(SUM(worker_slots),0) FROM control.batch_worker
				WHERE status IN ('IDLE','BUSY') AND last_heartbeat_at >= :cutoff
				""", java.util.Map.of("cutoff", java.sql.Timestamp.from(cutoff)), Integer.class);
		return result == null ? 0 : result;
	}

	public List<BatchWorker> active(Instant cutoff) {
		return this.jdbc.query(
				"SELECT * FROM control.batch_worker WHERE status IN ('IDLE','BUSY') AND last_heartbeat_at >= :cutoff",
				java.util.Map.of("cutoff", java.sql.Timestamp.from(cutoff)), this::map);
	}

	private static MapSqlParameterSource args(BatchWorker worker) {
		return new MapSqlParameterSource().addValue("id", worker.workerId())
			.addValue("pod", worker.podName())
			.addValue("ns", worker.podNamespace())
			.addValue("version", worker.applicationVersion())
			.addValue("status", worker.status().name())
			.addValue("slots", worker.workerSlots())
			.addValue("now", java.sql.Timestamp.from(worker.startedAt()));
	}

	private BatchWorker map(ResultSet rs, int ignored) throws SQLException {
		return new BatchWorker(rs.getString("worker_id"), rs.getString("pod_name"), rs.getString("pod_namespace"),
				rs.getString("application_version"), WorkerStatus.valueOf(rs.getString("status")),
				rs.getInt("worker_slots"), rs.getTimestamp("started_at").toInstant(),
				rs.getTimestamp("last_heartbeat_at").toInstant(), (Long) rs.getObject("current_job_execution"),
				(Long) rs.getObject("current_partition_id"), rs.getBoolean("shutdown_requested"));
	}

}
