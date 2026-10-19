package com.flashsale.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.net.SocketException;

import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * What the rate limiter does when Redis is unreachable, too slow or resets the connection: it steps aside
 * ("fails open"), so buyers are served uncounted. And switched off, it never calls Redis.
 */
class RateLimiterFailureTest {

    private static final RateLimitProperties ON = new RateLimitProperties(true, 10, 5);

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @Test
    void ifRedisIsDownEveryRequestGoesThroughUncounted() {
        RateLimiter limiter = new RateLimiter(redisThatFails(new RedisConnectionFailureException("Redis is down")),
                ON, meters);

        RateLimiter.Decision decision = limiter.tryAcquire("alice");

        assertThat(decision.allowed()).isTrue();
        assertThat(decision.counted()).as("no X-RateLimit headers: there is no real number to show").isFalse();
        assertThat(meters.counter("flashsale.rate.limit", "outcome", "redis_down").count()).isEqualTo(1);
    }

    @Test
    void ifRedisIsTooSlowOrResetsTheConnectionTheRequestGoesThroughToo() {
        assertThat(new RateLimiter(redisThatFails(new QueryTimeoutException("Redis command timed out")), ON, meters)
                .tryAcquire("alice").allowed()).isTrue();
        assertThat(new RateLimiter(redisThatFails(new RedisSystemException("Redis exception",
                new SocketException("Connection reset"))), ON, meters).tryAcquire("alice").allowed()).isTrue();
    }

    @Test
    void switchedOffItNeverCallsRedis() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RateLimiter limiter = new RateLimiter(redis, new RateLimitProperties(false, 10, 5), meters);

        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        verifyNoInteractions(redis);
    }

    /** A fake StringRedisTemplate where EVERY method throws the given exception. */
    private static StringRedisTemplate redisThatFails(RuntimeException failure) {
        return mock(StringRedisTemplate.class, invocation -> {
            throw failure;
        });
    }
}
