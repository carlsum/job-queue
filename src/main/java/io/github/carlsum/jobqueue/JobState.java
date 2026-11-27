package io.github.carlsum.jobqueue;

import java.time.Instant;

/**
 * A snapshot of a job's row, for monitoring and tests.
 *
 * @param id the job's ID
 * @param queue the queue it belongs to
 * @param status where it is in its life
 * @param attempts how many times it has been claimed so far
 * @param maxAttempts how many attempts it is allowed
 * @param runAt when it may run next (for queued jobs)
 * @param lastError the most recent failure, or null if it has never failed
 */
public record JobState(
        long id, String queue, JobStatus status, int attempts, int maxAttempts, Instant runAt, String lastError) {}
