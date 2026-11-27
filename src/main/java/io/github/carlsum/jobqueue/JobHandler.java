package io.github.carlsum.jobqueue;

/**
 * Does the work for one job.
 *
 * <p>Returning normally marks the job succeeded. Throwing marks the attempt failed, and the job
 * is retried later unless that was its last attempt.
 *
 * <p>A job can run more than once: if a worker dies, or finishes after its lease ran out, the job
 * runs again elsewhere. Handlers must therefore be idempotent, for example by recording what they
 * have done in the same database transaction as their side effects, or by passing an idempotency
 * key to the services they call.
 */
@FunctionalInterface
public interface JobHandler {

    void handle(Job job) throws Exception;
}
