package io.github.carlsum.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JobWorkerTest {

    private static final String PAYLOAD = "{}";

    private static final WorkerOptions FAST = WorkerOptions.defaults()
            .withConcurrency(4)
            .withPollInterval(Duration.ofMillis(20))
            .withRetryPolicy(new RetryPolicy(Duration.ofMillis(10), Duration.ofMillis(50)))
            .withShutdownTimeout(Duration.ofSeconds(5));

    private final JobQueue jobs = new JobQueue(TestDatabase.dataSource());
    private final List<JobWorker> workers = new ArrayList<>();

    @BeforeEach
    void emptyTheQueue() {
        TestDatabase.reset();
    }

    @AfterEach
    void stopWorkers() {
        workers.forEach(JobWorker::close);
    }

    @Test
    void runsEveryJob() {
        enqueue(50);
        Queue<Long> handled = new ConcurrentLinkedQueue<>();

        start(job -> handled.add(job.id()));

        await().until(() -> count(JobStatus.SUCCEEDED) == 50);
        assertThat(handled).hasSize(50).doesNotHaveDuplicates();
    }

    @Test
    void retriesAFailingJobUntilItSucceeds() {
        long id = jobs.enqueue(NewJob.of("emails", PAYLOAD));
        AtomicInteger calls = new AtomicInteger();

        start(job -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("mail server busy");
            }
        });

        await().until(() -> state(id).status() == JobStatus.SUCCEEDED);
        assertThat(state(id).attempts()).isEqualTo(3);
        assertThat(state(id).lastError()).contains("mail server busy");
    }

    @Test
    void aJobThatAlwaysFailsEndsUpDead() {
        long id = jobs.enqueue(NewJob.of("emails", PAYLOAD).withMaxAttempts(2));

        start(job -> {
            throw new IllegalStateException("no such address");
        });

        await().until(() -> state(id).status() == JobStatus.DEAD);
        assertThat(state(id).attempts()).isEqualTo(2);
    }

    @Test
    void neverRunsMoreJobsAtOnceThanItsConcurrency() {
        enqueue(20);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger mostAtOnce = new AtomicInteger();

        start(job -> {
            mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
            Thread.sleep(50);
            running.decrementAndGet();
        });

        await().until(() -> count(JobStatus.SUCCEEDED) == 20);
        assertThat(mostAtOnce.get()).isBetween(2, FAST.concurrency());
    }

    @Test
    void aJobAbandonedByACrashedWorkerIsRunByAnother() {
        long id = jobs.enqueue(NewJob.of("emails", PAYLOAD));
        // A worker claims the job with a short lease, then "crashes": it never reports back.
        jobs.claim("emails", "crashed-worker", 1, Duration.ofMillis(100));

        start(job -> {});

        await().until(() -> state(id).status() == JobStatus.SUCCEEDED);
        assertThat(state(id).attempts()).isEqualTo(2);
        assertThat(state(id).lastError()).contains("lease expired on crashed-worker");
    }

    @Test
    void closeLetsRunningJobsFinish() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        JobWorker worker = start(job -> {
            started.countDown();
            Thread.sleep(300);
        });
        long id = jobs.enqueue(NewJob.of("emails", PAYLOAD));
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        worker.close();

        assertThat(state(id).status()).isEqualTo(JobStatus.SUCCEEDED);
    }

    private JobWorker start(JobHandler handler) {
        JobWorker worker = JobWorker.start(jobs, "emails", handler, FAST);
        workers.add(worker);
        return worker;
    }

    private void enqueue(int count) {
        for (int i = 0; i < count; i++) {
            jobs.enqueue(NewJob.of("emails", PAYLOAD));
        }
    }

    private JobState state(long id) {
        return jobs.find(id).orElseThrow();
    }

    private static long count(JobStatus status) {
        return TestDatabase.queryLong("SELECT count(*) FROM job_queue WHERE status = ?", status.name().toLowerCase());
    }
}
