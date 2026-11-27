package io.github.carlsum.jobqueue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * A durable job queue stored in one PostgreSQL table (see {@code db/jobqueue/V1__create_job_queue.sql}).
 *
 * <p>Jobs are claimed with {@code SELECT ... FOR UPDATE SKIP LOCKED}, so any number of workers in
 * any number of processes can poll the same queue without claiming the same job twice and without
 * waiting on each other's locks.
 *
 * <p>A claimed job is leased. If its worker neither completes nor fails it before the lease runs
 * out (because the process crashed, say), {@link #reapExpiredLeases()} puts it back on the queue.
 * Delivery is therefore at least once, and handlers must be idempotent.
 *
 * <p>All times come from the database clock ({@code now()}), never the JVM's, so workers on
 * machines whose clocks differ slightly still agree on when jobs are due and leases expire.
 */
public final class JobQueue {

    private static final int MAX_ERROR_LENGTH = 8_000;

    private static final String INSERT = """
            INSERT INTO job_queue (queue, payload, run_at, max_attempts, idempotency_key)
            VALUES (?, CAST(? AS jsonb), now() + make_interval(secs => ?), ?, ?)
            ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            RETURNING id
            """;

    private static final String FIND_BY_IDEMPOTENCY_KEY = """
            SELECT id FROM job_queue WHERE queue = ? AND idempotency_key = ?
            """;

    // SKIP LOCKED: rows another worker is claiming right now are skipped instead of waited for.
    private static final String CLAIM = """
            WITH next AS (
                SELECT id FROM job_queue
                WHERE queue = ? AND status = 'queued' AND run_at <= now()
                ORDER BY run_at, id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            UPDATE job_queue AS j
            SET status = 'running',
                attempts = j.attempts + 1,
                locked_by = ?,
                locked_until = now() + make_interval(secs => ?),
                updated_at = now()
            FROM next
            WHERE j.id = next.id
            RETURNING j.id, j.queue, j.payload::text, j.attempts, j.max_attempts
            """;

    // "attempts = ?" is the fencing check: it only matches the attempt that holds the lease now.
    private static final String COMPLETE = """
            UPDATE job_queue
            SET status = 'succeeded', locked_by = NULL, locked_until = NULL,
                finished_at = now(), updated_at = now()
            WHERE id = ? AND status = 'running' AND attempts = ?
            """;

    private static final String FAIL = """
            UPDATE job_queue
            SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
                run_at = CASE WHEN attempts >= max_attempts THEN run_at
                              ELSE now() + make_interval(secs => ?) END,
                last_error = ?,
                finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
                locked_by = NULL, locked_until = NULL, updated_at = now()
            WHERE id = ? AND status = 'running' AND attempts = ?
            """;

    // A crashed worker counts as a failed attempt, so a job that crashes its worker every time
    // still ends up dead instead of looping forever.
    private static final String REAP = """
            UPDATE job_queue
            SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
                run_at = now(),
                last_error = 'lease expired on ' || locked_by
                             || ': the worker stopped, or the job ran longer than its lease',
                finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
                locked_by = NULL, locked_until = NULL, updated_at = now()
            WHERE status = 'running' AND locked_until <= now()
            """;

    private static final String FIND = """
            SELECT id, queue, status, attempts, max_attempts, run_at, last_error
            FROM job_queue WHERE id = ?
            """;

    private final DataSource dataSource;

    public JobQueue(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * Adds a job in its own transaction.
     *
     * @return the new job's ID, or the existing job's ID if its idempotency key was already used
     */
    public long enqueue(NewJob job) {
        Objects.requireNonNull(job, "job");
        return withConnection("enqueue a job on " + job.queue(), connection -> insert(connection, job));
    }

    /**
     * Adds a job inside the caller's transaction, without committing it. The job only becomes
     * visible to workers if the transaction commits, so it can never run for an order (say) that
     * was rolled back, and never goes missing for one that was committed.
     *
     * @return the new job's ID, or the existing job's ID if its idempotency key was already used
     */
    public long enqueue(Connection transaction, NewJob job) {
        Objects.requireNonNull(transaction, "transaction");
        Objects.requireNonNull(job, "job");
        try {
            return insert(transaction, job);
        } catch (SQLException e) {
            throw new JobQueueException("could not enqueue a job on " + job.queue(), e);
        }
    }

    /**
     * Claims up to {@code limit} due jobs from {@code queue}, leasing each to {@code workerId}
     * for {@code lease}. Each must then be passed to {@link #complete} or {@link #fail} before the
     * lease runs out.
     */
    public List<Job> claim(String queue, String workerId, int limit, Duration lease) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(lease, "lease");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1: " + limit);
        }
        if (lease.isNegative() || lease.isZero()) {
            throw new IllegalArgumentException("lease must be positive: " + lease);
        }
        return withConnection("claim jobs from " + queue, connection -> {
            try (PreparedStatement claim = connection.prepareStatement(CLAIM)) {
                claim.setString(1, queue);
                claim.setInt(2, limit);
                claim.setString(3, workerId);
                claim.setDouble(4, seconds(lease));
                List<Job> jobs = new ArrayList<>();
                try (ResultSet rows = claim.executeQuery()) {
                    while (rows.next()) {
                        jobs.add(new Job(rows.getLong(1), rows.getString(2), rows.getString(3), rows.getInt(4), rows.getInt(5)));
                    }
                }
                return jobs;
            }
        });
    }

    /**
     * Marks a claimed job succeeded.
     *
     * @return false if this attempt no longer holds the job (its lease expired), in which case
     *     nothing changed
     */
    public boolean complete(Job job) {
        Objects.requireNonNull(job, "job");
        return withConnection("complete job " + job.id(), connection -> {
            try (PreparedStatement complete = connection.prepareStatement(COMPLETE)) {
                complete.setLong(1, job.id());
                complete.setInt(2, job.attempt());
                return complete.executeUpdate() == 1;
            }
        });
    }

    /**
     * Records a failed attempt. The job runs again after {@code retryDelay}, or is marked dead if
     * this was its last attempt.
     *
     * @return false if this attempt no longer holds the job (its lease expired), in which case
     *     nothing changed
     */
    public boolean fail(Job job, Throwable error, Duration retryDelay) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(error, "error");
        Objects.requireNonNull(retryDelay, "retryDelay");
        return withConnection("fail job " + job.id(), connection -> {
            try (PreparedStatement fail = connection.prepareStatement(FAIL)) {
                fail.setDouble(1, seconds(retryDelay));
                fail.setString(2, describe(error));
                fail.setLong(3, job.id());
                fail.setInt(4, job.attempt());
                return fail.executeUpdate() == 1;
            }
        });
    }

    /**
     * Puts running jobs whose lease has expired back on the queue, or marks them dead if that was
     * their last attempt. Workers call this regularly; any number may call it at once.
     *
     * @return how many jobs were released
     */
    public int reapExpiredLeases() {
        return withConnection("reap expired leases", connection -> {
            try (PreparedStatement reap = connection.prepareStatement(REAP)) {
                return reap.executeUpdate();
            }
        });
    }

    /** Looks up a job by ID. */
    public Optional<JobState> find(long id) {
        return withConnection("find job " + id, connection -> {
            try (PreparedStatement find = connection.prepareStatement(FIND)) {
                find.setLong(1, id);
                try (ResultSet row = find.executeQuery()) {
                    if (!row.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new JobState(
                            row.getLong("id"),
                            row.getString("queue"),
                            JobStatus.fromDatabase(row.getString("status")),
                            row.getInt("attempts"),
                            row.getInt("max_attempts"),
                            row.getObject("run_at", OffsetDateTime.class).toInstant(),
                            row.getString("last_error")));
                }
            }
        });
    }

    private static long insert(Connection connection, NewJob job) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
            insert.setString(1, job.queue());
            insert.setString(2, job.payload());
            insert.setDouble(3, seconds(job.delay()));
            insert.setInt(4, job.maxAttempts());
            insert.setString(5, job.idempotencyKey());
            try (ResultSet row = insert.executeQuery()) {
                if (row.next()) {
                    return row.getLong(1);
                }
            }
        }
        // Nothing was inserted, so the idempotency key is taken: return the job that has it.
        try (PreparedStatement find = connection.prepareStatement(FIND_BY_IDEMPOTENCY_KEY)) {
            find.setString(1, job.queue());
            find.setString(2, job.idempotencyKey());
            try (ResultSet row = find.executeQuery()) {
                if (row.next()) {
                    return row.getLong(1);
                }
            }
        }
        throw new SQLException("job with idempotency key " + job.idempotencyKey() + " was neither inserted nor found");
    }

    /**
     * Runs {@code work} on a connection of its own in auto-commit mode, so each statement commits
     * at once even if the pool hands out connections with auto-commit off.
     */
    private <T> T withConnection(String action, SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            return work.run(connection);
        } catch (SQLException e) {
            throw new JobQueueException("could not " + action, e);
        }
    }

    @FunctionalInterface
    private interface SqlWork<T> {
        T run(Connection connection) throws SQLException;
    }

    private static double seconds(Duration duration) {
        return duration.getSeconds() + duration.getNano() / 1e9;
    }

    private static String describe(Throwable error) {
        StringWriter trace = new StringWriter();
        error.printStackTrace(new PrintWriter(trace));
        String text = trace.toString();
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }
}
