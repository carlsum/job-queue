package io.github.carlsum.jobqueue;

import static io.github.carlsum.jobqueue.Concurrency.runConcurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JobQueueTest {

    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final String PAYLOAD = "{\"orderId\": 42}";

    private final DataSource db = TestDatabase.dataSource();
    private final JobQueue jobs = new JobQueue(db);

    @BeforeEach
    void emptyTheQueue() {
        TestDatabase.reset();
    }

    @Test
    void aJobIsClaimedExactlyOnce() {
        long id = jobs.enqueue(NewJob.of("emails", PAYLOAD));

        List<Job> claimed = jobs.claim("emails", "worker-1", 10, LEASE);

        assertThat(claimed).containsExactly(new Job(id, "emails", PAYLOAD, 1, NewJob.DEFAULT_MAX_ATTEMPTS));
        assertThat(jobs.claim("emails", "worker-2", 10, LEASE)).isEmpty();
        assertThat(state(id).status()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void workersOnlySeeTheirOwnQueue() {
        jobs.enqueue(NewJob.of("emails", PAYLOAD));

        assertThat(jobs.claim("invoices", "worker-1", 10, LEASE)).isEmpty();
    }

    @Test
    void theOldestDueJobIsClaimedFirst() {
        long first = jobs.enqueue(NewJob.of("emails", PAYLOAD));
        jobs.enqueue(NewJob.of("emails", PAYLOAD));

        assertThat(jobs.claim("emails", "worker-1", 1, LEASE)).extracting(Job::id).containsExactly(first);
    }

    @Test
    void aDelayedJobIsNotClaimedBeforeItIsDue() {
        long id = jobs.enqueue(NewJob.of("emails", PAYLOAD).withDelay(Duration.ofHours(1)));
        assertThat(jobs.claim("emails", "worker-1", 10, LEASE)).isEmpty();

        makeDue(id);
        assertThat(jobs.claim("emails", "worker-1", 10, LEASE)).extracting(Job::id).containsExactly(id);
    }

    @Test
    void anIdempotencyKeyIsOnlyEnqueuedOncePerQueue() {
        NewJob email = NewJob.of("emails", PAYLOAD).withIdempotencyKey("order-42-confirmation");
        NewJob sms = new NewJob("sms", PAYLOAD, Duration.ZERO, "order-42-confirmation", 5);

        long first = jobs.enqueue(email);
        long again = jobs.enqueue(email);
        long otherQueue = jobs.enqueue(sms);

        assertThat(again).isEqualTo(first);
        assertThat(otherQueue).isNotEqualTo(first);
        assertThat(TestDatabase.queryLong("SELECT count(*) FROM job_queue")).isEqualTo(2);
    }

    @Test
    void aJobEnqueuedInATransactionAppearsOnlyWhenItCommits() throws SQLException {
        try (Connection transaction = db.getConnection()) {
            transaction.setAutoCommit(false);
            long id = jobs.enqueue(transaction, NewJob.of("emails", PAYLOAD));
            assertThat(jobs.claim("emails", "worker-1", 10, LEASE)).isEmpty();

            transaction.commit();
            assertThat(jobs.claim("emails", "worker-1", 10, LEASE)).extracting(Job::id).containsExactly(id);
        }
    }

    @Test
    void aJobEnqueuedInARolledBackTransactionNeverExists() throws SQLException {
        try (Connection transaction = db.getConnection()) {
            transaction.setAutoCommit(false);
            jobs.enqueue(transaction, NewJob.of("emails", PAYLOAD));
            transaction.rollback();
        }

        assertThat(TestDatabase.queryLong("SELECT count(*) FROM job_queue")).isZero();
    }

    @Test
    void rejectsAPayloadThatIsNotJson() {
        assertThatThrownBy(() -> jobs.enqueue(NewJob.of("emails", "not json")))
                .isInstanceOf(JobQueueException.class);
    }

    @Test
    void completingAJobMarksItSucceeded() {
        Job job = enqueueAndClaim(NewJob.of("emails", PAYLOAD));

        assertThat(jobs.complete(job)).isTrue();
        assertThat(state(job.id()).status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(jobs.complete(job)).as("completing twice").isFalse();
    }

    @Test
    void aFailedJobIsRetriedAfterTheDelay() {
        Job job = enqueueAndClaim(NewJob.of("emails", PAYLOAD));

        assertThat(jobs.fail(job, new IllegalStateException("mail server down"), Duration.ofMinutes(5))).isTrue();

        JobState state = state(job.id());
        assertThat(state.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(state.lastError()).contains("mail server down");
        assertThat(state.runAt()).isAfter(Instant.now().plus(Duration.ofMinutes(4)));
        assertThat(jobs.claim("emails", "worker-1", 10, LEASE)).isEmpty();

        makeDue(job.id());
        assertThat(jobs.claim("emails", "worker-1", 10, LEASE)).singleElement()
                .extracting(Job::attempt).isEqualTo(2);
    }

    @Test
    void aJobThatFailsItsLastAttemptIsDead() {
        Job job = enqueueAndClaim(NewJob.of("emails", PAYLOAD).withMaxAttempts(1));
        assertThat(job.isLastAttempt()).isTrue();

        jobs.fail(job, new IllegalStateException("no such address"), Duration.ofMinutes(5));

        assertThat(state(job.id()).status()).isEqualTo(JobStatus.DEAD);
        makeDue(job.id());
        assertThat(jobs.claim("emails", "worker-1", 10, LEASE)).isEmpty();
    }

    @Test
    void anExpiredLeaseIsReclaimedAndTheOldAttemptCanNoLongerFinish() {
        Job first = enqueueAndClaim(NewJob.of("emails", PAYLOAD)); // leased to worker-1
        expireLeases();

        assertThat(jobs.reapExpiredLeases()).isEqualTo(1);
        assertThat(state(first.id()).lastError()).contains("lease expired on worker-1");
        Job second = jobs.claim("emails", "worker-2", 10, LEASE).get(0);
        assertThat(second.attempt()).isEqualTo(2);

        // worker-1 was only slow, not dead, and now reports back. The attempt number no longer
        // matches, so it cannot overwrite what the second attempt does.
        assertThat(jobs.complete(first)).isFalse();
        assertThat(jobs.fail(first, new IllegalStateException("too late"), Duration.ZERO)).isFalse();
        assertThat(jobs.complete(second)).isTrue();
    }

    @Test
    void anExpiredLeaseOnTheLastAttemptMakesTheJobDead() {
        Job job = enqueueAndClaim(NewJob.of("emails", PAYLOAD).withMaxAttempts(1));
        expireLeases();

        jobs.reapExpiredLeases();

        assertThat(state(job.id()).status()).isEqualTo(JobStatus.DEAD);
    }

    @Test
    void reapingLeavesLiveLeasesAlone() {
        Job job = enqueueAndClaim(NewJob.of("emails", PAYLOAD));

        assertThat(jobs.reapExpiredLeases()).isZero();
        assertThat(state(job.id()).status()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void concurrentWorkersNeverClaimTheSameJob() throws Exception {
        int total = 500;
        try (Connection transaction = db.getConnection()) {
            transaction.setAutoCommit(false);
            for (int i = 0; i < total; i++) {
                jobs.enqueue(transaction, NewJob.of("emails", "{\"n\": " + i + "}"));
            }
            transaction.commit();
        }
        Queue<Long> claimed = new ConcurrentLinkedQueue<>();
        AtomicInteger workerNumber = new AtomicInteger();

        runConcurrently(8, () -> {
            String workerId = "worker-" + workerNumber.incrementAndGet();
            List<Job> batch;
            while (!(batch = jobs.claim("emails", workerId, 10, LEASE)).isEmpty()) {
                batch.forEach(job -> claimed.add(job.id()));
            }
        });

        assertThat(claimed).hasSize(total).doesNotHaveDuplicates();
    }

    private Job enqueueAndClaim(NewJob newJob) {
        long id = jobs.enqueue(newJob);
        List<Job> claimed = jobs.claim(newJob.queue(), "worker-1", 10, LEASE);
        assertThat(claimed).extracting(Job::id).containsExactly(id);
        return claimed.get(0);
    }

    private JobState state(long id) {
        return jobs.find(id).orElseThrow();
    }

    private static void makeDue(long id) {
        TestDatabase.execute("UPDATE job_queue SET run_at = now() WHERE id = ?", id);
    }

    /** Simulates time passing: every running job's lease ended a second ago. */
    private static void expireLeases() {
        TestDatabase.execute(
                "UPDATE job_queue SET locked_until = now() - interval '1 second' WHERE status = 'running'");
    }
}
