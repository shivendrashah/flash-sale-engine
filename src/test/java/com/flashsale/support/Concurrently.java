package com.flashsale.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;

/**
 * Runs the same task on many threads that all start at the same instant, to provoke race conditions.
 * Used by OversellTest (50 buyers), IdempotencyTest (double-clicks) and HoldExpiryTest (two expiry jobs).
 */
public final class Concurrently {

    private Concurrently() {
    }

    public static <T> List<Outcome<T>> run(int threads, IntFunction<T> task) {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch everyoneReady = new CountDownLatch(threads); // each thread counts down once it's ready
            CountDownLatch startingGun = new CountDownLatch(1);         // the main thread fires it once
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int threadNumber = i;
                futures.add(pool.submit(() -> {
                    everyoneReady.countDown();
                    startingGun.await(); // all threads wait here, so they really start together
                    return task.apply(threadNumber);
                }));
            }
            everyoneReady.await();   // wait until every thread is parked at the starting line...
            startingGun.countDown(); // ...then release them all at once

            List<Outcome<T>> outcomes = new ArrayList<>();
            for (Future<T> future : futures) {
                try {
                    outcomes.add(Outcome.success(future.get(60, TimeUnit.SECONDS)));
                } catch (ExecutionException e) {
                    outcomes.add(Outcome.failure(e.getCause())); // the exception the task threw (e.g. SoldOutException)
                }
            }
            return outcomes;
        } catch (InterruptedException | TimeoutException e) {
            throw new IllegalStateException("Concurrent run did not finish", e);
        } finally {
            pool.shutdownNow();
        }
    }

    public static <T> long successes(List<Outcome<T>> outcomes) {
        return outcomes.stream().filter(Outcome::succeeded).count();
    }

    public static <T> List<Throwable> failures(List<Outcome<T>> outcomes) {
        return outcomes.stream().filter(outcome -> !outcome.succeeded()).map(Outcome::error).toList();
    }

    /** What one thread ended with: a value or an exception. */
    public record Outcome<T>(T value, Throwable error) {

        static <T> Outcome<T> success(T value) {
            return new Outcome<>(value, null);
        }

        static <T> Outcome<T> failure(Throwable error) {
            return new Outcome<>(null, error);
        }

        public boolean succeeded() {
            return error == null;
        }
    }
}
