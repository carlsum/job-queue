# job-queue

[![CI](https://github.com/carlsum/job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/carlsum/job-queue/actions/workflows/ci.yml)

A durable background job queue for Java 21, stored in PostgreSQL. There's no message broker to run:
jobs live in a table next to your data, and can be enqueued in the same transaction.

- **Safe with many workers**: jobs are claimed with `FOR UPDATE SKIP LOCKED`, so no two workers get the same job.
- **Survives crashes**: a claimed job is leased; if its worker dies, the job runs again elsewhere.
- **Fenced**: a slow worker whose lease ran out cannot overwrite the newer attempt's result.
- **Transactional enqueue**: a job exists only if your business transaction commits.
- **Idempotency keys**, delayed jobs, retries with jittered exponential backoff, and a dead state.
- **Virtual-thread workers** with a concurrency limit and graceful shutdown.
- **No runtime dependencies**: plain JDBC. Bring your own driver and connection pool.

## Installation

Published through [JitPack](https://jitpack.io/#carlsum/job-queue). Requires Java 21+.

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.github.carlsum</groupId>
  <artifactId>job-queue</artifactId>
  <version>v0.1.0</version>
</dependency>
```

## Usage

```java
JobQueue jobs = new JobQueue(dataSource);

// Enqueue in the same transaction as the business change.
try (Connection tx = dataSource.getConnection()) {
    tx.setAutoCommit(false);
    orders.insert(tx, order);
    jobs.enqueue(tx, NewJob.of("emails", "{\"orderId\": 42}")
            .withIdempotencyKey("order-42-confirmation"));
    tx.commit();
}

// Run jobs. A handler that throws is retried later.
JobWorker worker = JobWorker.start(jobs, "emails",
        job -> mailer.sendConfirmation(job.payload()),
        WorkerOptions.defaults().withConcurrency(20));

// On shutdown: stop claiming, let running jobs finish.
worker.close();
```

Create the table with [`V1__create_job_queue.sql`](src/main/resources/db/jobqueue/V1__create_job_queue.sql):
copy it into your migrations, or add `classpath:db/jobqueue` to Flyway's locations.

## Job lifecycle

```
             claim                complete
  queued ──────────────► running ───────────► succeeded
    ▲                     │   │
    │  fail, or lease     │   │  fail, or lease expired,
    └──── expired ────────┘   └── on the last attempt ──► dead
```

## Design notes

**Why PostgreSQL instead of a broker.** Most apps already have PostgreSQL, and a job stored in the
same database can be enqueued in the same transaction as the change that caused it. Then a job never
runs for an order that was rolled back, and never goes missing for one that was committed. With a
separate broker you'd need an outbox table to get the same guarantee. `SKIP LOCKED` lets PostgreSQL
handle thousands of jobs per second. A broker becomes the better choice when throughput is far beyond
that, or when many independent consumers need the same events.

**Claiming with `SKIP LOCKED`.** Each worker selects the oldest due jobs, locks them and marks them
running, all in one statement. `SKIP LOCKED` makes concurrent workers pass over rows another worker
is claiming instead of queueing behind its lock. A test runs 8 workers against 500 jobs and checks
each job is claimed exactly once; with `SKIP LOCKED` removed, that test fails.

**Leases and fencing.** Claiming a job leases it to the worker until `locked_until`. If the worker
dies, the lease runs out and the job is put back (or marked dead if that was its last attempt). The
attempt number acts as a fencing token: `complete` and `fail` only match `attempts = <my attempt>`.
So a worker that was merely slow, and reports back after its job was claimed again, changes nothing.

**At-least-once delivery.** Because of leases, a job can run twice (for example, a worker finishes the
work but dies before recording it). Exactly-once is impossible once a handler has side effects outside
the database, so handlers must be idempotent.

**The database clock.** Due times and lease expiry use PostgreSQL's `now()`, never the JVM clock, so
workers on machines with slightly different clocks still agree.

**Backoff with full jitter.** After attempt n fails, the retry waits a random time between zero and
`min(max, base * 2^(n-1))`. When many jobs fail together (a dependency is down), the randomness
spreads their retries out instead of hitting the recovering dependency in waves.

**Virtual threads and a semaphore.** Every job runs on its own virtual thread, so blocking I/O in
handlers is cheap. Concurrency is limited by a semaphore instead of a pool size. The poller claims only
as many jobs as there are free slots, so no job's lease starts before there's capacity to run it.

**Stopping without interrupts.** On a virtual thread, interrupting a blocking socket call closes the
socket. The first version stopped the poller with `Thread.interrupt()`, which killed its JDBC
connection mid-query (and would return dead connections to a pool). Now `close()` signals a latch, and
every wait in the poll loop times out regularly, so the poller is never interrupted.

**Indexes.** Partial indexes cover only queued jobs (for claiming) and running jobs (for expired leases).
Finished jobs, eventually almost every row, don't make either index bigger.

## Testing

Tests run against a real PostgreSQL 18, started from a Maven dependency
([embedded-postgres](https://github.com/zonkyio/embedded-postgres)): no Docker or local install
needed. Instead of sleeping, tests simulate time by editing rows, for example moving `locked_until`
into the past to expire a lease.

```
mvn verify
```

## Possible next steps

- `LISTEN`/`NOTIFY` so new jobs wake workers immediately instead of waiting for the next poll
- Lease heartbeats, so jobs can run longer than their lease
- Errors that skip retries and go straight to dead (for example, invalid payloads)
- Retention: delete succeeded jobs after a while, or partition the table by time
- Requeueing dead jobs, and Micrometer metrics (queue depth, age of the oldest due job)
- Priorities and recurring (cron) jobs
- A Spring Boot starter and a web dashboard

## License

[MIT](LICENSE)
