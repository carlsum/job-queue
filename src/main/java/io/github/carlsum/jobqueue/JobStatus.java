package io.github.carlsum.jobqueue;

import java.util.Locale;

/** Where a job is in its life. */
public enum JobStatus {
    /** Waiting to run, now or at a later time. */
    QUEUED,
    /** Claimed by a worker that holds a lease on it. */
    RUNNING,
    /** Finished successfully. */
    SUCCEEDED,
    /** Failed on its last attempt; it will not run again unless someone requeues it. */
    DEAD;

    static JobStatus fromDatabase(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }
}
