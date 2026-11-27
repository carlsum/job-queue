package io.github.carlsum.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(10));

    @Test
    void theLongestDelayDoublesEachAttemptUpToTheMax() {
        assertThat(policy.longestDelayAfterAttempt(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.longestDelayAfterAttempt(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.longestDelayAfterAttempt(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.longestDelayAfterAttempt(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(policy.longestDelayAfterAttempt(5)).isEqualTo(Duration.ofSeconds(10));
        assertThat(policy.longestDelayAfterAttempt(5_000)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void delaysAreSpreadRandomlyBetweenZeroAndTheLongest() {
        Random random = new Random(42);

        List<Duration> delays = IntStream.range(0, 1_000)
                .mapToObj(i -> policy.delayAfterAttempt(3, random))
                .toList();

        assertThat(delays).allSatisfy(delay -> assertThat(delay).isBetween(Duration.ZERO, Duration.ofSeconds(4)));
        assertThat(delays).anySatisfy(delay -> assertThat(delay).isLessThan(Duration.ofSeconds(1)));
        assertThat(delays).anySatisfy(delay -> assertThat(delay).isGreaterThan(Duration.ofSeconds(3)));
    }

    @Test
    void rejectsInvalidSettings() {
        assertThatThrownBy(() -> new RetryPolicy(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(Duration.ofSeconds(2), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy.delayAfterAttempt(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
