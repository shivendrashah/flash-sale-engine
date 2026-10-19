package com.flashsale.common;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Small "sleep for a while" helpers.
 */
public final class Pause {

    private Pause() {
    }

    public static void of(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            return;
        }
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while pausing", e);
        }
    }

    /**
     * Sleeps a random time between {@code minMillis} and {@code maxMillis}. The randomness ("jitter") stops threads
     * that just collided from retrying at the same moment and colliding again.
     */
    public static void randomMillis(long minMillis, long maxMillis) {
        long upper = Math.max(minMillis + 1, maxMillis);
        of(Duration.ofMillis(ThreadLocalRandom.current().nextLong(minMillis, upper)));
    }
}
