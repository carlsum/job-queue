package io.github.carlsum.jobqueue;

/** The database could not carry out a queue operation. Wraps the underlying {@link java.sql.SQLException}. */
public class JobQueueException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JobQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
