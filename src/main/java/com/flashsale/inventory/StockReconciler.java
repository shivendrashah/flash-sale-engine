package com.flashsale.inventory;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The stock reconciliation job. For every item of a live sale, it compares the stock gate's counter in Redis
 * with available_stock in PostgreSQL (the source of truth), and:
 *   they match                                  -> nothing to do
 *   they differ                                 -> report it (log, metrics) and remember what it saw
 *   Redis is LOW, the same way on the next run  -> real drift: set the counter to PostgreSQL's number
 *   Redis is HIGH                               -> report only; the counter's 60 s expiry heals it
 * The gate already heals itself (a counter expires 60 s after it is seeded), so this job is mostly a
 * measuring tool: it makes drift visible instead of hiding it behind the 60 s healing, and fixes it sooner.
 *
 * Why only a LOW counter is fixed: the job can't tell real drift from a purchase or an expiry that is half done
 * (Redis takes a unit a few milliseconds BEFORE PostgreSQL commits; after an expiry, PostgreSQL gets the unit back a
 * moment BEFORE Redis does). So any fix may be a mistake. Raising a low counter to PostgreSQL's number can, at worst,
 * leave it a little HIGH, which is harmless: PostgreSQL still decides every sale, a few extra buyers reach it, and the
 * expiry heals the counter. Lowering a high counter could leave it LOW, and a low counter turns buyers away while
 * units are left. So the job only ever moves a counter in the safe direction. (Mistaken fixes are real: with the job
 * running non-stop during a sale, StockReconcilerTest saw one leave a counter one unit high, 140 vs 139.)
 *
 * Why a difference must be seen twice: most differences ARE in flight, and each mistaken fix costs a few extra trips
 * to PostgreSQL. A difference still exactly the same on the next run (10 s later) is very likely real.
 *
 *               no-op. Each instance remembers its own last run (the map below), which is all it needs.
 */
@Service
public class StockReconciler {

    private static final Logger log = LoggerFactory.getLogger(StockReconciler.class);

    private static final RedisScript<Long> FIX = new DefaultRedisScript<>("""
            -- KEYS[1] = a gate counter, ARGV[1] = the value the job saw, ARGV[2] = PostgreSQL's available_stock
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                redis.call('SET', KEYS[1], ARGV[2], 'KEEPTTL')
                return 1
            end
            return 0
            """, Long.class);

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final MeterRegistry meters;
    private final Clock clock;

    private final Map<String, Observation> lastRun = new ConcurrentHashMap<>();

    /** Units of drift seen in the last run, summed over all items (the gauge). */
    private final AtomicLong driftUnits = new AtomicLong();

    public StockReconciler(JdbcTemplate jdbc, StringRedisTemplate redis, MeterRegistry meters, Clock clock) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.meters = meters;
        this.clock = clock;
        Gauge.builder("flashsale.stock.drift", driftUnits, AtomicLong::get)
                .description("Units by which the stock gate's counters differed from PostgreSQL in the last run")
                .register(meters);
    }

    /**
     * @return what this run found
     */
    public Result reconcile() {
        Instant now = clock.instant();
        Map<Long, List<ItemStock>> itemsBySale = new LinkedHashMap<>();
        jdbc.query("""
                SELECT si.sale_id, si.id, si.available_stock
                  FROM sale_items si JOIN sales s ON s.id = si.sale_id
                 WHERE s.starts_at <= ? AND s.ends_at > ?
                 ORDER BY si.sale_id, si.id
                """, row -> {
                    itemsBySale.computeIfAbsent(row.getLong(1), id -> new ArrayList<>())
                            .add(new ItemStock(row.getLong(1), row.getLong(2), row.getInt(3)));
                }, Timestamp.from(now), Timestamp.from(now));

        int checked = 0;
        int drifted = 0;
        int fixed = 0;
        long units = 0;
        Map<String, Observation> seen = new HashMap<>();
        try {
            for (List<ItemStock> items : itemsBySale.values()) {
                List<String> keys = items.stream().map(ItemStock::key).toList();
                List<String> counters = redis.opsForValue().multiGet(keys);
                for (int i = 0; i < items.size(); i++) {
                    String counter = counters == null ? null : counters.get(i);
                    if (counter == null) {
                        continue;     // no counter: nobody bought recently, or it expired. Nothing to compare.
                    }
                    checked++;
                    ItemStock item = items.get(i);
                    Observation observed = new Observation(Integer.parseInt(counter), item.availableStock());
                    if (observed.matches()) {
                        count("match");
                        continue;
                    }
                    drifted++;
                    units += Math.abs(observed.drift());
                    if (observed.drift() > 0) {
                        count("high");
                        continue;
                    }
                    if (observed.equals(lastRun.get(item.key()))) {
                        if (fix(item.key(), observed)) {
                            fixed++;
                            count("fixed");
                            log.warn("Stock gate counter {} was {} but PostgreSQL has {} (drift {}): fixed",
                                    item.key(), observed.redis(), observed.postgres(), observed.drift());
                        } else {
                            count("changed_meanwhile");   // a buyer moved it, or it expired: look again next run
                        }
                    } else {
                        count("low");
                        seen.put(item.key(), observed);
                        log.info("Stock gate counter {} is {} but PostgreSQL has {}; will fix it if still so next run",
                                item.key(), observed.redis(), observed.postgres());
                    }
                }
            }
        } catch (DataAccessException redisDown) {
            log.warn("Stock reconciliation skipped: Redis unreachable or too slow ({})", redisDown.getMessage());
            count("redis_down");
            lastRun.clear();
            return new Result(0, 0, 0);
        }
        lastRun.clear();
        lastRun.putAll(seen);
        driftUnits.set(units);
        return new Result(checked, drifted, fixed);
    }

    /** Forgets the last run's differences (tests: each test starts with a job that has seen nothing). */
    void forgetLastRun() {
        lastRun.clear();
    }

    /** Compare-and-set: PostgreSQL's number, only if the counter still holds what the job saw. */
    boolean fix(String key, Observation seen) {
        Long done = redis.execute(FIX, List.of(key), String.valueOf(seen.redis()), String.valueOf(seen.postgres()));
        return done != null && done == 1;
    }

    private void count(String outcome) {
        meters.counter("flashsale.stock.reconcile", "outcome", outcome).increment();
    }

    /** What one run found: counters compared, counters that differed from PostgreSQL, counters fixed. */
    public record Result(int checked, int drifted, int fixed) {
    }

    /** One look at one item: the gate's counter and PostgreSQL's available_stock. */
    record Observation(int redis, int postgres) {

        boolean matches() {
            return redis == postgres;
        }

        /** Negative: Redis is LOW, so buyers are told "sold out" while units are left (lost sales). */
        int drift() {
            return redis - postgres;
        }
    }

    private record ItemStock(long saleId, long saleItemId, int availableStock) {

        String key() {
            return StockGate.key(saleId, saleItemId);
        }
    }
}
