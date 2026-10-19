package com.flashsale.waitingroom;

import java.time.Clock;
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

import com.flashsale.catalog.Sale;

/**
 * The waiting room. For a sale with admitPerSecond set, buyers first join a queue; every 250 ms the admitter
 * lets the next ones in, at most admitPerSecond a second, and each gets a PASS that lasts 5 minutes. Only a
 * buyer with a pass can create a hold.
 *   50,000 people click "Buy" at 10:00:00  ->  the checkout sees 200 a second, in the order they arrived
 * Everything after the waiting room (the gate, the locks, PostgreSQL) then works at a steady, planned load.
 */
@Component
public class WaitingRoom {

    private static final Logger log = LoggerFactory.getLogger(WaitingRoom.class);

    /** A position the scripts return for a buyer who already holds a pass. */
    private static final long ADMITTED = -1;
    /** A position the STATUS script returns for a buyer who is neither queued nor admitted. */
    private static final long NOT_QUEUED = -2;

    private static final RedisScript<Long> JOIN = new DefaultRedisScript<>("""
            if redis.call('EXISTS', KEYS[3]) == 1 then
                return -1
            end
            local place = redis.call('ZRANK', KEYS[1], ARGV[1])
            if not place then
                -- a turn number from one counter: two instances can't hand out the same turn, and
                --               no clock is involved, so "first come, first served" is exact
                redis.call('ZADD', KEYS[1], redis.call('INCR', KEYS[2]), ARGV[1])
                redis.call('PEXPIRE', KEYS[1], ARGV[2])
                redis.call('PEXPIRE', KEYS[2], ARGV[2])
                place = redis.call('ZRANK', KEYS[1], ARGV[1])
            end
            return place
            """, Long.class);

    private static final RedisScript<Long> STATUS = new DefaultRedisScript<>("""
            if redis.call('EXISTS', KEYS[2]) == 1 then
                return -1
            end
            local place = redis.call('ZRANK', KEYS[1], ARGV[1])
            if not place then
                return -2
            end
            return place
            """, Long.class);

    private static final RedisScript<Long> ADMIT = new DefaultRedisScript<>("""
            local rate = tonumber(ARGV[1])
            local time = redis.call('TIME')                  -- Redis's clock: the same for every instance
            local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            local bucket = redis.call('HMGET', KEYS[2], 'credit', 'at')
            -- credit = how many buyers may go in now; it grows by `rate` a second, up to one second's worth
            local credit = tonumber(bucket[1]) or rate
            local at = tonumber(bucket[2]) or now
            if now > at then
                credit = math.min(rate, credit + (now - at) * rate / 1000)
                at = now
            end
            local admitted = 0
            local room = math.floor(credit)
            if room > 0 then
                local popped = redis.call('ZPOPMIN', KEYS[1], room)   -- the first `room` buyers in line
                for i = 1, #popped, 2 do                               -- {member, score, member, score, ...}
                    redis.call('SET', ARGV[3] .. popped[i], '1', 'PX', ARGV[2])
                    admitted = admitted + 1
                end
                credit = credit - admitted
            end
            redis.call('HSET', KEYS[2], 'credit', credit, 'at', at)
            redis.call('PEXPIRE', KEYS[2], 60000)
            return admitted
            """, Long.class);

    private final StringRedisTemplate redis;
    private final WaitingRoomProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;

    public WaitingRoom(StringRedisTemplate redis, WaitingRoomProperties properties, MeterRegistry meters,
                       Clock clock) {
        this.redis = redis;
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
    }

    public static String queueKey(long saleId) {
        return "wr:{" + saleId + "}:queue";
    }

    public static String passKey(long saleId, String userId) {
        return passPrefix(saleId) + userId;
    }

    static String admitKey(long saleId) {
        return "wr:{" + saleId + "}:admit";
    }

    private static String turnKey(long saleId) {
        return "wr:{" + saleId + "}:turn";
    }

    private static String passPrefix(long saleId) {
        return "wr:{" + saleId + "}:pass:";
    }

    /**
     * A sale without a waiting room needs no line: the buyer is told they're in.
     */
    public QueueStatus join(Sale sale, String userId) {
        if (!sale.hasWaitingRoom()) {
            return QueueStatus.admitted(sale.getId());
        }
        try {
            Long place = redis.execute(JOIN, List.of(queueKey(sale.getId()), turnKey(sale.getId()),
                    passKey(sale.getId(), userId)), userId, String.valueOf(keysLifetime(sale).toMillis()));
            count("joined");
            return toStatus(sale, place);
        } catch (DataAccessException redisDown) {
            return failOpen(sale, "join", redisDown);
        }
    }

    /** Where the buyer stands, without joining. */
    public QueueStatus status(Sale sale, String userId) {
        if (!sale.hasWaitingRoom()) {
            return QueueStatus.admitted(sale.getId());
        }
        try {
            Long place = redis.execute(STATUS, List.of(queueKey(sale.getId()), passKey(sale.getId(), userId)),
                    userId);
            return toStatus(sale, place);
        } catch (DataAccessException redisDown) {
            return failOpen(sale, "status", redisDown);
        }
    }

    /**
     * @throws NotAdmittedException 403 NOT_ADMITTED, if the buyer has no pass (never joined, still waiting, or the
     *                              pass ran out)
     */
    public void requirePass(Sale sale, String userId) {
        if (!sale.hasWaitingRoom()) {
            return;
        }
        boolean hasPass;
        try {
            hasPass = Boolean.TRUE.equals(redis.hasKey(passKey(sale.getId(), userId)));
        } catch (DataAccessException redisDown) {
            failOpen(sale, "pass check", redisDown);
            return;
        }
        if (!hasPass) {
            count("refused");
            throw new NotAdmittedException(sale.getId());
        }
    }

    /**
     * @return how many were let in by this call
     */
    public int admit(Sale sale) {
        Long admitted = redis.execute(ADMIT, List.of(queueKey(sale.getId()), admitKey(sale.getId())),
                String.valueOf(sale.getAdmitPerSecond()), String.valueOf(properties.passTtl().toMillis()),
                passPrefix(sale.getId()));
        int count = admitted == null ? 0 : admitted.intValue();
        if (count > 0) {
            meters.counter("flashsale.waiting_room", "outcome", "admitted").increment(count);
        }
        return count;
    }

    private QueueStatus toStatus(Sale sale, Long place) {
        if (place == null || place == ADMITTED) {
            return QueueStatus.admitted(sale.getId());
        }
        if (place == NOT_QUEUED) {
            return QueueStatus.notQueued(sale.getId());
        }
        long position = place + 1;                       // 0-based rank -> "you are number 1 in line"
        long waitSeconds = (position + sale.getAdmitPerSecond() - 1) / sale.getAdmitPerSecond();   // rounded up
        return QueueStatus.waiting(sale.getId(), position, waitSeconds);
    }

    private QueueStatus failOpen(Sale sale, String what, DataAccessException redisDown) {
        log.warn("Waiting room skipped for sale {} ({}): Redis unreachable or too slow: {}", sale.getId(), what,
                redisDown.getMessage());
        count("redis_down");
        return QueueStatus.admitted(sale.getId());
    }

    /** The queue keys outlive the sale by an hour, then Redis deletes them (no clean-up job needed). */
    private Duration keysLifetime(Sale sale) {
        Duration untilEnd = Duration.between(clock.instant(), sale.getEndsAt());
        return (untilEnd.isNegative() ? Duration.ZERO : untilEnd).plus(Duration.ofHours(1));
    }

    private void count(String outcome) {
        meters.counter("flashsale.waiting_room", "outcome", outcome).increment();
    }

    /** For tests, the admitter's log and the demo page (SaleStatsService): buyers still in line. */
    public long queueLength(long saleId) {
        Long size = redis.opsForZSet().zCard(queueKey(saleId));
        return size == null ? 0 : size;
    }
}
