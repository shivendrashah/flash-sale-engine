package com.flashsale.ratelimit;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * A token bucket per buyer. Each buyer has a bucket of tokens (10 by default); every request that changes
 * something spends one; the bucket refills at a steady rate (5 tokens a second). An empty bucket means
 * 429 Too Many Requests, with Retry-After saying how long until the next token.
 *   a person clicking:  never notices (a burst of 10 is more than any human sends)
 *   a script hammering: gets 5 requests a second, and a 429 for the rest, before any database work
 *
 * Why a token bucket and not "N per minute" (a fixed window)? A fixed window lets a client send N requests at 0:59
 * and N more at 1:00: twice the limit within a second. A bucket allows one burst (its capacity) and then a steady
 * rate, with no edge to time against. It also needs no cleanup job: a bucket nobody uses expires by itself.
 */
@Component
public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> TOKEN_BUCKET = new DefaultRedisScript<>("""
            -- A token bucket for one buyer.
            --   KEYS[1]  the bucket, e.g. rl:alice
            --   ARGV[1]  capacity: the most tokens the bucket holds
            --   ARGV[2]  refill: tokens added back per second
            -- Answers {1 or 0 (allowed?), whole tokens left, milliseconds until the next token (0 if allowed)}
            local capacity = tonumber(ARGV[1])
            local perMs = tonumber(ARGV[2]) / 1000
            -- Redis's own clock, so every app instance measures time the same way
            local time = redis.call('TIME')   -- {seconds, microseconds}
            local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'at')
            -- A missing bucket (a new buyer, or one idle long enough to expire) is a full one
            local tokens = tonumber(bucket[1]) or capacity
            local at = tonumber(bucket[2]) or now
            if now > at then
                -- add what dripped in since `at`, but never more than the capacity
                tokens = math.min(capacity, tokens + (now - at) * perMs)
                at = now
            end
            local allowed = 0
            local waitMs = 0
            if tokens >= 1 then
                tokens = tokens - 1
                allowed = 1
            else
                waitMs = math.ceil((1 - tokens) / perMs)   -- how long until one whole token has dripped in
            end
            redis.call('HSET', KEYS[1], 'tokens', tokens, 'at', at)
            -- Once the bucket would be full again, it equals a missing one: let Redis delete it
            redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / perMs))
            return {allowed, math.floor(tokens), waitMs}
            """, List.class);

    private final StringRedisTemplate redis;
    private final RateLimitProperties properties;
    private final MeterRegistry meters;

    public RateLimiter(StringRedisTemplate redis, RateLimitProperties properties, MeterRegistry meters) {
        this.redis = redis;
        this.properties = properties;
        this.meters = meters;
    }

    /** The Redis key of a buyer's bucket: "rl:alice". */
    public static String key(String userId) {
        return "rl:" + userId;
    }

    /**
     * @return allowed or not, plus what the response headers need. Never throws: if Redis is down, the request is
     *         allowed and not counted.
     */
    public Decision tryAcquire(String userId) {
        if (!properties.enabled()) {
            return Decision.NOT_COUNTED;
        }
        return tryAcquire(userId, properties.capacity(), properties.refillPerSecond());
    }

    /** The same, with a bucket size and rate of the caller's choice (tests use a very slow refill). */
    Decision tryAcquire(String userId, int capacity, double refillPerSecond) {
        List<?> answer;
        try {
            answer = redis.execute(TOKEN_BUCKET, List.of(key(userId)),
                    String.valueOf(capacity), String.valueOf(refillPerSecond));
        } catch (DataAccessException redisDown) {
            log.warn("Rate limit skipped for {} (Redis unreachable or too slow: {})", userId, redisDown.getMessage());
            count("redis_down");
            return Decision.NOT_COUNTED;
        }
        boolean allowed = number(answer, 0) == 1;
        count(allowed ? "allowed" : "limited");
        return new Decision(allowed, true, capacity, (int) number(answer, 1),
                Duration.ofMillis(number(answer, 2)));
    }

    /** A Lua number comes back as a Long (Lua decimals are cut to whole numbers on the way out). */
    private static long number(List<?> answer, int index) {
        return ((Number) answer.get(index)).longValue();
    }

    /** One more request for this outcome: allowed, limited or redis_down. */
    private void count(String outcome) {
        meters.counter("flashsale.rate.limit", "outcome", outcome).increment();
    }

    /**
     * What the limiter decided for one request.
     *
     * @param allowed    true = go on; false = answer 429
     * @param counted    false when the limiter is switched off or Redis was down (then the rest means nothing, and
     *                   no X-RateLimit headers are sent)
     * @param limit      the bucket's capacity (X-RateLimit-Limit)
     * @param remaining  whole tokens left after this request (X-RateLimit-Remaining)
     * @param retryAfter how long until the next token, when not allowed
     */
    public record Decision(boolean allowed, boolean counted, int limit, int remaining, Duration retryAfter) {

        static final Decision NOT_COUNTED = new Decision(true, false, 0, 0, Duration.ZERO);

        /** Retry-After in whole seconds, rounded up: 200 ms becomes 1 (the header can't say less than 1 second). */
        public int retryAfterSeconds() {
            return (int) Math.max(1, (retryAfter.toMillis() + 999) / 1000);
        }
    }
}
