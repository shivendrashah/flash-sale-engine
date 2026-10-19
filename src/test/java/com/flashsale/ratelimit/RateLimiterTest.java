package com.flashsale.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.flashsale.support.Concurrently;
import com.flashsale.support.Concurrently.Outcome;
import com.flashsale.support.IntegrationTest;

/**
 * The token bucket: a burst up to the capacity, then a steady refill; never more than the capacity; one
 * bucket per buyer; a bucket nobody uses expires; and 20 threads of one buyer at once still get exactly the
 * burst, not one more.
 */
class RateLimiterTest extends IntegrationTest {

    /** One token every 100 seconds: in a test that runs for milliseconds, the bucket never refills. */
    private static final double NO_REFILL = 0.01;

    @Autowired
    RateLimiter rateLimiter;

    @Test
    void aBurstUpToTheCapacityGoesThroughAndTheNextRequestWaits() {
        List<RateLimiter.Decision> burst = IntStream.range(0, 11)
                .mapToObj(i -> rateLimiter.tryAcquire("alice", 10, NO_REFILL))
                .toList();

        assertThat(burst.subList(0, 10)).allMatch(RateLimiter.Decision::allowed);
        assertThat(burst.stream().map(RateLimiter.Decision::remaining).toList())
                .as("X-RateLimit-Remaining counts down").containsExactly(9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0);
        RateLimiter.Decision eleventh = burst.get(10);
        assertThat(eleventh.allowed()).isFalse();
        assertThat(eleventh.retryAfter()).as("one token every 100 s").isBetween(Duration.ofSeconds(99),
                Duration.ofSeconds(100));
        assertThat(eleventh.retryAfterSeconds()).isEqualTo(100);
    }

    @Test
    void theBucketRefillsAtTheConfiguredRate() {
        // application.yml: capacity 10, refill-per-second 5
        IntStream.range(0, 10).forEach(i -> assertThat(rateLimiter.tryAcquire("alice").allowed()).isTrue());
        RateLimiter.Decision empty = rateLimiter.tryAcquire("alice");
        assertThat(empty.allowed()).isFalse();
        assertThat(empty.retryAfter()).as("one token every 200 ms").isLessThanOrEqualTo(Duration.ofMillis(200));
        assertThat(empty.retryAfterSeconds()).as("the header can't say less than 1 s").isEqualTo(1);

        ageBucket("alice", Duration.ofSeconds(1));   // "one second later": 5 tokens dripped in

        assertThat(IntStream.range(0, 6).mapToObj(i -> rateLimiter.tryAcquire("alice").allowed()))
                .containsExactly(true, true, true, true, true, false);
    }

    @Test
    void anIdleBucketNeverHoldsMoreThanItsCapacity() {
        rateLimiter.tryAcquire("alice");
        ageBucket("alice", Duration.ofHours(1));     // an hour of refill would be 18,000 tokens

        long allowed = IntStream.range(0, 15).filter(i -> rateLimiter.tryAcquire("alice").allowed()).count();

        assertThat(allowed).isEqualTo(10);
    }

    @Test
    void eachBuyerHasTheirOwnBucket() {
        IntStream.range(0, 10).forEach(i -> rateLimiter.tryAcquire("alice", 10, NO_REFILL));
        assertThat(rateLimiter.tryAcquire("alice", 10, NO_REFILL).allowed()).isFalse();

        assertThat(rateLimiter.tryAcquire("bob", 10, NO_REFILL).allowed()).isTrue();
    }

    @Test
    void aBucketNobodyUsesExpiresOnceItWouldBeFullAgain() {
        rateLimiter.tryAcquire("alice");

        Long ttl = redis.getExpire(RateLimiter.key("alice"), TimeUnit.MILLISECONDS);
        assertThat(ttl).isBetween(1L, 2000L);
    }

    @Test
    void twentyRequestsAtOnceGetExactlyTheBurst() {
        List<Outcome<RateLimiter.Decision>> outcomes =
                Concurrently.run(20, thread -> rateLimiter.tryAcquire("alice", 10, NO_REFILL));

        assertThat(outcomes).allMatch(Outcome::succeeded);
        assertThat(outcomes.stream().filter(outcome -> outcome.value().allowed())).hasSize(10);
    }
}
