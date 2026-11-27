package io.github.carlsum.jobqueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class Concurrency {

    private Concurrency() {}

    /**
     * Runs {@code task} on {@code threads} threads, all released at the same moment to make
     * contention as likely as possible, and waits for them to finish.
     */
    static void runConcurrently(int threads, Runnable task) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    task.run();
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS); // rethrows anything a worker threw
            }
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError(e);
        } finally {
            pool.shutdownNow();
        }
    }
}
