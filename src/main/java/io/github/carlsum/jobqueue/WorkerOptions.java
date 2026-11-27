package io.github.carlsum.jobqueue;

import java.time.Duration;
import java.util.Objects;

/**
 * How a {@link JobWorker} runs.
 *
 * @param concurrency most jobs this worker runs at once
 * @param pollInterval how long to wait before polling again when the queue had no due jobs
 * @param lease how long a worker may hold a job before other workers consider it abandoned and
 *     run it again. Must be longer than the slowest job.
 * @param retryPolicy how long to wait before retrying a failed job
 * @param shutdownTimeout how long {@link JobWorker#close()} waits for running jobs before
 *     interrupting them
 */
public record WorkerOptions(
        int concurrency, Duration pollInterval, Duration lease, RetryPolicy retryPolicy, Duration shutdownTimeout) {

    public WorkerOptions {
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be at least 1: " + concurrency);
        }
        requirePositive(pollInterval, "pollInterval");
        requirePositive(lease, "lease");
        requirePositive(shutdownTimeout, "shutdownTimeout");
    }

    /** 10 jobs at once, 1 s polls, 5 min leases, retries between 1 s and 15 min apart. */
    public static WorkerOptions defaults() {
        return new WorkerOptions(
                10,
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                new RetryPolicy(Duration.ofSeconds(1), Duration.ofMinutes(15)),
                Duration.ofSeconds(30));
    }

    public WorkerOptions withConcurrency(int concurrency) {
        return new WorkerOptions(concurrency, pollInterval, lease, retryPolicy, shutdownTimeout);
    }

    public WorkerOptions withPollInterval(Duration pollInterval) {
        return new WorkerOptions(concurrency, pollInterval, lease, retryPolicy, shutdownTimeout);
    }

    public WorkerOptions withLease(Duration lease) {
        return new WorkerOptions(concurrency, pollInterval, lease, retryPolicy, shutdownTimeout);
    }

    public WorkerOptions withRetryPolicy(RetryPolicy retryPolicy) {
        return new WorkerOptions(concurrency, pollInterval, lease, retryPolicy, shutdownTimeout);
    }

    public WorkerOptions withShutdownTimeout(Duration shutdownTimeout) {
        return new WorkerOptions(concurrency, pollInterval, lease, retryPolicy, shutdownTimeout);
    }

    private static void requirePositive(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException(name + " must be positive: " + duration);
        }
    }
}
