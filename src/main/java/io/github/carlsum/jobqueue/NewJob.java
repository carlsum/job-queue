package io.github.carlsum.jobqueue;

import java.time.Duration;
import java.util.Objects;

/**
 * A job to put on a queue.
 *
 * @param queue name of the queue; workers poll one queue each
 * @param payload the job's input as JSON text. Serializing it is the caller's job, so this
 *     library needs no JSON dependency. PostgreSQL stores it as {@code jsonb}, which normalizes
 *     whitespace and key order.
 * @param delay how long to wait before the job may run
 * @param idempotencyKey if not null, enqueueing another job with the same key on the same queue
 *     returns the existing job instead of adding a second one
 * @param maxAttempts how many times the job may run before it is marked dead
 */
public record NewJob(String queue, String payload, Duration delay, String idempotencyKey, int maxAttempts) {

    public static final int DEFAULT_MAX_ATTEMPTS = 5;

    public NewJob {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(delay, "delay");
        if (queue.isBlank()) {
            throw new IllegalArgumentException("queue must not be blank");
        }
        if (delay.isNegative()) {
            throw new IllegalArgumentException("delay must not be negative: " + delay);
        }
        if (idempotencyKey != null && idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must be null or not blank");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1: " + maxAttempts);
        }
    }

    /** A job that can run now, with no idempotency key and the default number of attempts. */
    public static NewJob of(String queue, String payload) {
        return new NewJob(queue, payload, Duration.ZERO, null, DEFAULT_MAX_ATTEMPTS);
    }

    public NewJob withDelay(Duration delay) {
        return new NewJob(queue, payload, delay, idempotencyKey, maxAttempts);
    }

    public NewJob withIdempotencyKey(String idempotencyKey) {
        return new NewJob(queue, payload, delay, idempotencyKey, maxAttempts);
    }

    public NewJob withMaxAttempts(int maxAttempts) {
        return new NewJob(queue, payload, delay, idempotencyKey, maxAttempts);
    }
}
