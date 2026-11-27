package io.github.carlsum.jobqueue;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with "full jitter": after attempt n fails, wait a random time between zero
 * and {@code min(maxDelay, baseDelay * 2^(n-1))}.
 *
 * <p>The randomness matters when many jobs fail together, for example while a service they call
 * is down. Without it they would all retry at the same moments and hit the service in waves just
 * as it recovers.
 *
 * @param baseDelay the longest wait after the first failure
 * @param maxDelay the cap on the longest wait, however many failures
 */
public record RetryPolicy(Duration baseDelay, Duration maxDelay) {

    public RetryPolicy {
        Objects.requireNonNull(baseDelay, "baseDelay");
        Objects.requireNonNull(maxDelay, "maxDelay");
        if (baseDelay.isNegative() || baseDelay.isZero()) {
            throw new IllegalArgumentException("baseDelay must be positive: " + baseDelay);
        }
        if (maxDelay.compareTo(baseDelay) < 0) {
            throw new IllegalArgumentException("maxDelay must be at least baseDelay");
        }
        if (maxDelay.compareTo(Duration.ofDays(365)) > 0) {
            throw new IllegalArgumentException("maxDelay must be at most a year: " + maxDelay);
        }
    }

    /** A random delay to wait after {@code attempt} (1 or more) has failed. */
    public Duration delayAfterAttempt(int attempt) {
        return delayAfterAttempt(attempt, ThreadLocalRandom.current());
    }

    /** Like {@link #delayAfterAttempt(int)}, drawing randomness from {@code random}. */
    public Duration delayAfterAttempt(int attempt, RandomGenerator random) {
        return Duration.ofNanos((long) (random.nextDouble() * longestDelayAfterAttempt(attempt).toNanos()));
    }

    /** The longest delay {@link #delayAfterAttempt(int)} can return for {@code attempt}. */
    public Duration longestDelayAfterAttempt(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1: " + attempt);
        }
        // Math.pow overflows to infinity for huge attempt numbers, and min() then picks maxDelay.
        double exponential = baseDelay.toNanos() * Math.pow(2, attempt - 1);
        return Duration.ofNanos((long) Math.min(maxDelay.toNanos(), exponential));
    }
}
