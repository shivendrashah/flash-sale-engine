package com.flashsale.inventory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import com.flashsale.common.Pause;

/**
 * A simple distributed lock on Redis: "only one app instance at a time may work on this key".
 *
 * <ul>
 *   <li>Lock: on the wire this is {@code SET lock:sale-item:7 <random-token> EX 5 NX}.
 *       NX = only set it if the key doesn't exist yet (nobody holds the lock).
 *       EX 5 = delete the key automatically after 5 seconds, so the lock frees itself if the holder crashes.</li>
 *   <li>Unlock: delete the key only if it still holds OUR random token. Checking and deleting happen inside one Lua
 *       script, and Redis runs a script without interruption, so nobody can sneak in between. Without the token
 *       check, a slow holder whose lock already expired could delete the NEXT holder's lock.</li>
 * </ul>
 */
@Component
public class RedisLock {

    private static final Logger log = LoggerFactory.getLogger(RedisLock.class);

    private static final RedisScript<Long> RELEASE_IF_OWNER = new DefaultRedisScript<>("""
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;

    public RedisLock(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** Tries to take the lock, retrying until {@code maxWait} runs out. Empty means "someone else still has it". */
    public Optional<Lease> tryAcquire(String key, Duration ttl, Duration maxWait) {
        String token = UUID.randomUUID().toString(); // random, so only WE can unlock it
        long deadline = System.nanoTime() + maxWait.toNanos();
        while (true) {
            if (Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key, token, ttl))) {
                return Optional.of(new Lease(key, token));
            }
            if (System.nanoTime() >= deadline) {
                return Optional.empty(); // waited long enough: the caller answers 503 STOCK_BUSY
            }
            Pause.randomMillis(2, 10); // someone else has it; try again in a few milliseconds
        }
    }

    /**
     * Returns true if we still owned the lock and released it; false if it had already expired.
     * Best effort: if Redis is unreachable we log and move on. The expiry frees the lock anyway, and the caller's
     * work has already committed, so failing the request now would only hide a success.
     */
    public boolean release(Lease lease) {
        try {
            Long deleted = redis.execute(RELEASE_IF_OWNER, List.of(lease.key()), lease.token());
            return deleted != null && deleted == 1L;
        } catch (DataAccessException redisDown) {
            log.warn("Could not release {} (Redis unreachable or too slow); it will expire by its TTL", lease.key());
            return false;
        }
    }

    /** Proof that we hold the lock: the key, plus the random token only we know. */
    public record Lease(String key, String token) {
    }
}
