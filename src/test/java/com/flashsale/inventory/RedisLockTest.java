package com.flashsale.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.flashsale.support.IntegrationTest;

/**
 * Tests RedisLock against a real Redis (in a Testcontainers container).
 */
class RedisLockTest extends IntegrationTest {

    private static final String KEY = "lock:test";

    @Autowired
    RedisLock redisLock;

    @Test
    void onlyOneOwnerAtATime() {
        // maxWait = ZERO: try once and give up immediately if it's taken
        var first = redisLock.tryAcquire(KEY, Duration.ofSeconds(5), Duration.ZERO);
        var second = redisLock.tryAcquire(KEY, Duration.ofSeconds(5), Duration.ZERO);

        assertThat(first).isPresent();
        assertThat(second).isEmpty();
        assertThat(redisLock.release(first.orElseThrow())).isTrue();
        assertThat(redisLock.tryAcquire(KEY, Duration.ofSeconds(5), Duration.ZERO)).as("free again").isPresent();
    }

    @Test
    void onlyTheOwnerCanUnlock() {
        RedisLock.Lease lease = redisLock.tryAcquire(KEY, Duration.ofSeconds(5), Duration.ZERO).orElseThrow();

        // Someone with the wrong token can't delete our lock (the Lua script compares tokens).
        assertThat(redisLock.release(new RedisLock.Lease(KEY, "someone-elses-token"))).isFalse();
        assertThat(redisLock.tryAcquire(KEY, Duration.ofSeconds(5), Duration.ZERO)).as("still locked").isEmpty();
        assertThat(redisLock.release(lease)).isTrue();
    }

    @Test
    void aLockFreesItselfWhenItsOwnerDisappears() {
        // Taken and never released, as if the owner crashed. The expiry (200 ms here) cleans it up.
        redisLock.tryAcquire(KEY, Duration.ofMillis(200), Duration.ZERO).orElseThrow();

        assertThat(redisLock.tryAcquire(KEY, Duration.ofSeconds(5), Duration.ofSeconds(2)))
                .as("acquired once the dead owner's TTL ran out")
                .isPresent();
    }
}
