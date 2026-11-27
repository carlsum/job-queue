package io.github.carlsum.jobqueue;

/**
 * A job claimed by a worker.
 *
 * <p>{@code attempt} doubles as a fencing token. If this attempt's lease expires and the job is
 * claimed again, the attempt number moves on, and {@link JobQueue#complete} or
 * {@link JobQueue#fail} called with this (now stale) {@code Job} changes nothing.
 *
 * @param id the job's ID
 * @param queue the queue it was claimed from
 * @param payload the job's input as JSON text
 * @param attempt which attempt this is, starting at 1
 * @param maxAttempts how many attempts the job is allowed
 */
public record Job(long id, String queue, String payload, int attempt, int maxAttempts) {

    /** Whether a failure now will mark the job dead instead of retrying it. */
    public boolean isLastAttempt() {
        return attempt >= maxAttempts;
    }
}
