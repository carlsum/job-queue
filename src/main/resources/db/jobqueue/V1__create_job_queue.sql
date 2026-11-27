-- Job queue schema for PostgreSQL 12+.
-- Add it to your own migrations, or point Flyway at classpath:db/jobqueue.

CREATE TABLE job_queue (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue           text        NOT NULL,
    payload         jsonb       NOT NULL,
    status          text        NOT NULL DEFAULT 'queued'
                                CHECK (status IN ('queued', 'running', 'succeeded', 'dead')),
    attempts        integer     NOT NULL DEFAULT 0,
    max_attempts    integer     NOT NULL CHECK (max_attempts > 0),
    run_at          timestamptz NOT NULL DEFAULT now(),
    locked_by       text,
    locked_until    timestamptz,
    last_error      text,
    idempotency_key text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,

    -- A job has a lease exactly while it is running.
    CHECK ((status = 'running') = (locked_until IS NOT NULL))
);

-- Workers look for due jobs in one queue, oldest first. The index is partial, so finished jobs
-- (eventually almost every row) don't make it any bigger.
CREATE INDEX job_queue_due ON job_queue (queue, run_at, id) WHERE status = 'queued';

-- Finds running jobs whose lease has expired because their worker died.
CREATE INDEX job_queue_leases ON job_queue (locked_until) WHERE status = 'running';

-- At most one job per idempotency key in each queue.
CREATE UNIQUE INDEX job_queue_idempotency ON job_queue (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
