package io.github.carlsum.jobqueue;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs jobs from one queue until closed.
 *
 * <p>One virtual thread polls the queue, and each claimed job runs on a virtual thread of its own.
 * Virtual threads are cheap, so the limit on concurrent jobs comes from a semaphore rather than
 * the size of a thread pool: a job holds one of {@code concurrency} slots from the moment it is
 * claimed until its result is recorded. The poller only claims as many jobs as there are free
 * slots, so no job is claimed (starting its lease) before there is capacity to run it.
 *
 * <p>The poller is never interrupted to stop it. On a virtual thread, an interrupt during a
 * blocking socket call closes the socket, so interrupting a poller in the middle of a query would
 * break its database connection (and hand a dead connection back to the pool). Instead, every
 * wait in the poll loop times out after one poll interval, and {@link #close()} wakes it early
 * through a latch.
 */
public final class JobWorker implements AutoCloseable {

    private static final Logger LOG = System.getLogger(JobWorker.class.getName());

    private final JobQueue jobQueue;
    private final String queue;
    private final JobHandler handler;
    private final WorkerOptions options;
    private final String workerId;
    private final Semaphore slots;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Thread poller;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch stopRequested = new CountDownLatch(1);

    /** When to next look for expired leases. Only touched by the poller thread. */
    private long nextReapNanos = System.nanoTime();

    private JobWorker(JobQueue jobQueue, String queue, JobHandler handler, WorkerOptions options) {
        this.jobQueue = Objects.requireNonNull(jobQueue, "jobQueue");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.options = Objects.requireNonNull(options, "options");
        this.workerId = queue + "@" + ProcessHandle.current().pid() + "-"
                + Integer.toHexString(ThreadLocalRandom.current().nextInt());
        this.slots = new Semaphore(options.concurrency());
        this.poller = Thread.ofVirtual().name("job-worker-" + queue).unstarted(this::pollLoop);
    }

    /** Starts a worker that runs jobs from {@code queue} with {@code handler}. */
    public static JobWorker start(JobQueue jobQueue, String queue, JobHandler handler, WorkerOptions options) {
        JobWorker worker = new JobWorker(jobQueue, queue, handler, options);
        worker.poller.start();
        return worker;
    }

    /** The name this worker records on the jobs it claims. */
    public String workerId() {
        return workerId;
    }

    private void pollLoop() {
        long pollNanos = options.pollInterval().toNanos();
        try {
            while (running.get()) {
                if (!slots.tryAcquire(pollNanos, TimeUnit.NANOSECONDS)) {
                    continue; // every slot is still busy; go round to check for close()
                }
                int capacity = 1 + slots.drainPermits();
                List<Job> jobs = List.of();
                try {
                    reapExpiredLeasesNowAndThen();
                    jobs = jobQueue.claim(queue, workerId, capacity, options.lease());
                } catch (RuntimeException e) {
                    LOG.log(Level.WARNING, "Could not poll queue " + queue + "; will try again", e);
                } finally {
                    slots.release(capacity - jobs.size());
                }
                for (Job job : jobs) {
                    executor.execute(() -> run(job));
                }
                // A full batch means more jobs are probably waiting, so poll again straight away.
                // Otherwise wait one interval, or less if close() is called.
                if (jobs.size() < capacity) {
                    stopRequested.await(pollNanos, TimeUnit.NANOSECONDS);
                }
            }
        } catch (InterruptedException e) {
            // Nothing in this class interrupts the poller. If something else does, stop polling.
            Thread.currentThread().interrupt();
        }
    }

    private void reapExpiredLeasesNowAndThen() {
        long now = System.nanoTime();
        if (now - nextReapNanos >= 0) {
            nextReapNanos = now + options.pollInterval().toNanos();
            int reaped = jobQueue.reapExpiredLeases();
            if (reaped > 0) {
                LOG.log(Level.WARNING, "Released {0} job(s) whose lease expired", reaped);
            }
        }
    }

    private void run(Job job) {
        Exception failure = null;
        try {
            handler.handle(job);
        } catch (Exception e) {
            failure = e;
        }
        // If close() timed out and interrupted this job, still record its result now rather than
        // leave it to wait for its lease to expire. An interrupt flag left set would close the
        // database connection mid-query, so clear it while recording and restore it afterwards.
        boolean interrupted = Thread.interrupted();
        try {
            if (!record(job, failure)) {
                LOG.log(Level.WARNING, "Job {0} finished after its lease expired; its result was discarded", job.id());
            }
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Could not record the result of job " + job.id()
                    + "; it will run again once its lease expires", e);
        } finally {
            slots.release();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean record(Job job, Exception failure) {
        if (failure == null) {
            return jobQueue.complete(job);
        }
        Duration delay = options.retryPolicy().delayAfterAttempt(job.attempt());
        LOG.log(Level.WARNING, "Job " + job.id() + " failed on attempt " + job.attempt() + " of "
                + job.maxAttempts() + (job.isLastAttempt() ? "; giving up" : "; retrying in " + delay), failure);
        return jobQueue.fail(job, failure, delay);
    }

    /**
     * Stops claiming jobs and waits up to the shutdown timeout for running jobs to finish. Jobs
     * still running after that are interrupted; if they don't record a result, their leases
     * expire and they run again later.
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        stopRequested.countDown();
        try {
            poller.join();
            executor.shutdown();
            if (!executor.awaitTermination(options.shutdownTimeout().toNanos(), TimeUnit.NANOSECONDS)) {
                LOG.log(Level.WARNING, "Jobs on {0} still running after {1}; interrupting them",
                        queue, options.shutdownTimeout());
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
