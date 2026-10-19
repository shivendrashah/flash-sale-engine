package com.flashsale.inventory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.catalog.SaleItem;

/**
 * The Redis stock gate. Redis keeps one counter per sale item: how many units the gate still lets through.
 * Before a cart reaches PostgreSQL, ONE Lua script checks and takes every line of it at once.
 *   Redis says no:  the buyer gets 409 SOLD_OUT at once, and the stock row in PostgreSQL is never touched.
 *   Redis says yes: the normal database path runs and STILL checks. PostgreSQL has the final word.
 * So under a crowd of 10,000 buyers for 100 units, about 100 reach the stock row instead of 10,000.
 *
 * ONE rule keeps the counters in step with PostgreSQL's available_stock: every unit taken in Redis is also taken in
 * PostgreSQL, or it is given back. A crash between the two steps can still break it; then a counter ends up a little
 * too LOW (a few units go unsold, none are oversold) until it expires (flashsale.inventory.stock-gate-ttl) and the
 * next buyer re-seeds it from PostgreSQL.
 */
@Component
public class StockGate {

    private static final Logger log = LoggerFactory.getLogger(StockGate.class);

    private static final RedisScript<String> TAKE = new DefaultRedisScript<>("""
            -- The gate's "take" step for one cart.
            --   KEYS[1]      this request's marker (user + Idempotency-Key): set once the request has taken units
            --   KEYS[i + 1]  the counter of cart line i, lowest sale item id first (e.g. stock:{5}:7)
            --   ARGV[1]      ALL_OR_NOTHING or PARTIAL
            --   ARGV[2]      how long a new counter or marker lives, in milliseconds
            --   ARGV[2i+1]   units cart line i wants
            --   ARGV[2i+2]   available_stock of line i as this request just read it from PostgreSQL (the seed)
            -- Answers one character per line: 1 = taken, 0 = not enough left,
            --   - = enough, but nothing taken because another line was short (ALL_OR_NOTHING),
            --   = = a copy of this same request already took units: let it through and take nothing more
            local lines = #KEYS - 1
            if redis.call('EXISTS', KEYS[1]) == 1 then
                return string.rep('=', lines)
            end
            local left = {}
            for i = 1, lines do
                -- A missing counter (new sale, Redis restarted, or the counter expired) starts from PostgreSQL's
                -- number. NX = only if it is still missing: when many buyers race to seed it, the first one wins.
                redis.call('SET', KEYS[i + 1], ARGV[2 * i + 2], 'PX', ARGV[2], 'NX')
                left[i] = tonumber(redis.call('GET', KEYS[i + 1]))
            end
            local verdicts = {}
            if ARGV[1] == 'ALL_OR_NOTHING' then
                local everyLineFits = true
                for i = 1, lines do
                    if left[i] >= tonumber(ARGV[2 * i + 1]) then
                        verdicts[i] = '-'
                    else
                        verdicts[i] = '0'
                        everyLineFits = false
                    end
                end
                if not everyLineFits then
                    return table.concat(verdicts)   -- one line is short: take nothing at all
                end
            end
            local tookSome = false
            for i = 1, lines do
                local wanted = tonumber(ARGV[2 * i + 1])
                if left[i] >= wanted then
                    redis.call('DECRBY', KEYS[i + 1], wanted)
                    verdicts[i] = '1'
                    tookSome = true
                else
                    verdicts[i] = '0'
                end
            end
            if tookSome then
                redis.call('SET', KEYS[1], '1', 'PX', ARGV[2])   -- from now on, copies of this request take nothing
            end
            return table.concat(verdicts)
            """, String.class);

    private static final RedisScript<Long> GIVE_BACK = new DefaultRedisScript<>("""
            -- The gate's "give back" step.   KEYS[i] = a counter,   ARGV[i] = units to add back to it.
            -- Only counters that exist get the units. INCRBY on a missing key would CREATE it, holding just these
            -- units ("1 left" for an item that really has 100). The next buyer re-seeds a missing counter instead.
            local updated = 0
            for i = 1, #KEYS do
                if redis.call('EXISTS', KEYS[i]) == 1 then
                    redis.call('INCRBY', KEYS[i], ARGV[i])
                    updated = updated + 1
                end
            end
            return updated
            """, Long.class);

    private final StringRedisTemplate redis;
    private final InventoryProperties properties;
    private final MeterRegistry meters;

    public StockGate(StringRedisTemplate redis, InventoryProperties properties, MeterRegistry meters) {
        this.redis = redis;
        this.properties = properties;
        this.meters = meters;
    }

    /**
     * The Redis key of a sale item's counter: "stock:{5}:7" is sale item 7 of sale 5.
     */
    public static String key(long saleId, long saleItemId) {
        return "stock:{" + saleId + "}:" + saleItemId;
    }

    /**
     * The marker a request leaves once it has taken units, e.g. "stock:{5}:request:5:alice:double-click".
     * @param requestId CreateHoldCommand.requestId(): the user and the Idempotency-Key, as one string
     */
    static String requestKey(long saleId, String requestId) {
        return "stock:{" + saleId + "}:request:" + requestId;
    }

    /**
     * A short line goes to the policy, just as it would in the database: ALL_OR_NOTHING throws SoldOutException
     * (409, and Redis took nothing), PARTIAL skips the line. If nothing at all gets through, the policy's onFinished
     * rule refuses the cart (409) and PostgreSQL is never asked.
     *
     * <p>Copies of one request (a double-click: same user, same Idempotency-Key, at the same moment) all get past
     * HoldService's replay check, because nothing is saved yet. Without a marker, each copy would take its own units
     * and the copies Redis refused would hear SOLD_OUT for a purchase that succeeded. So the first copy to take
     * units leaves a marker, and later copies are let through WITHOUT taking anything. In PostgreSQL they queue
     * behind the first one and then get its hold back as a replay.
     *
     * @param requestId the user and the Idempotency-Key as one string (CreateHoldCommand.requestId())
     * @param itemsById the cart's sale items as HoldService just loaded them. Their available_stock seeds a
     *                  missing counter, so seeding costs no extra query.
     * @param requested true/false from the X-Stock-Gate header, or null for flashsale.inventory.stock-gate-enabled
     * @return the lines to try in PostgreSQL, and whether Redis really took them (then the caller must hand
     *         whatever PostgreSQL doesn't keep to giveBackUnused)
     */
    public Admission admit(long saleId, String requestId, List<StockLine> lines, Map<Long, SaleItem> itemsById,
                           ReservationPolicy policy, Boolean requested) {
        List<StockLine> ordered = StockLine.inIdOrder(lines);
        String requestKey = requestKey(saleId, requestId);
        boolean on = requested != null ? requested : properties.stockGateEnabled();
        if (!on) {
            count("off");
            return Admission.skipped(saleId, requestKey, ordered); // every line goes to PostgreSQL; Redis isn't asked
        }

        String verdicts;
        try {
            verdicts = redis.execute(TAKE, takeKeys(saleId, requestKey, ordered),
                    takeArgs(ordered, itemsById, policy.mode()));
        } catch (DataAccessException redisDown) {
            log.warn("Stock gate skipped for sale {} (Redis unreachable or too slow: {}); PostgreSQL decides alone",
                    saleId, redisDown.getMessage());
            count("redis_down");
            return Admission.skipped(saleId, requestKey, ordered);
        }
        if (log.isDebugEnabled()) {   // the "learn" profile turns this on (application-learn.yml)
            log.debug("Stock gate, sale {}: {} {} -> \"{}\"", saleId, policy.mode(), describe(ordered), verdicts);
        }
        if (verdicts.startsWith("=")) {
            count("copy");
            return Admission.skipped(saleId, requestKey, ordered);
        }

        List<StockLine> letThrough = new ArrayList<>();
        try {
            for (int i = 0; i < ordered.size(); i++) {
                char verdict = verdicts.charAt(i);
                if (verdict == '1') {
                    letThrough.add(ordered.get(i));       // Redis took these units; PostgreSQL will try them
                } else if (verdict == '0') {
                    policy.onShortage(ordered.get(i));    // ALL_OR_NOTHING throws SOLD_OUT; PARTIAL skips the line
                }
                // '-': this line had enough, but another one didn't, so Redis took nothing (ALL_OR_NOTHING)
            }
            policy.onFinished(letThrough, ordered);
        } catch (RuntimeException refused) {
            giveBack(saleId, letThrough); // usually nothing; covers a policy that refuses units Redis already took
            count("refused");
            throw refused;
        }
        count("passed");
        return new Admission(saleId, requestKey, List.copyOf(letThrough), true);
    }

    /**
     * keep goes back to its counter. That covers a rolled-back transaction, a line the database skipped (Redis's
     * number was too high) and a 503 STOCK_BUSY. If no hold was saved at all, the request's marker goes too, so a
     * later retry is treated as a fresh request.
     *
     * @param kept the lines of the hold this request ended with (its own, or the one a copy of it saved first);
     *             empty when no hold was saved
     */
    public void giveBackUnused(Admission admission, Collection<StockLine> kept) {
        if (!admission.takenInRedis()) {
            return; // the gate was off, or Redis was down: Redis took nothing, so there is nothing to give back
        }
        Set<Long> keptIds = kept.stream().map(StockLine::saleItemId).collect(Collectors.toSet());
        giveBack(admission.saleId(), admission.letThrough().stream()
                .filter(line -> !keptIds.contains(line.saleItemId()))
                .toList());
        if (kept.isEmpty()) {
            forget(admission.requestKey());
        }
    }

    /**
     * only AFTER the database transaction has committed.
     */
    public void giveBack(Collection<HeldStock> released) {
        Map<Long, Map<Long, Integer>> unitsBySale = new TreeMap<>();
        for (HeldStock stock : released) {
            for (StockLine line : stock.lines()) {
                unitsBySale.computeIfAbsent(stock.saleId(), saleId -> new TreeMap<>())
                        .merge(line.saleItemId(), line.quantity(), Integer::sum);
            }
        }
        unitsBySale.forEach((saleId, units) -> giveBack(saleId, units.entrySet().stream()
                .map(entry -> new StockLine(entry.getKey(), entry.getValue()))
                .toList()));
    }

    /**
     * Best effort, like RedisLock.release: if Redis is unreachable we log and move on. The database work has already
     * happened; failing the request now would hide a success. The counter is then a little low until it expires.
     */
    private void giveBack(long saleId, List<StockLine> lines) {
        if (lines.isEmpty()) {
            return;
        }
        try {
            redis.execute(GIVE_BACK, keys(saleId, lines),
                    lines.stream().map(line -> String.valueOf(line.quantity())).toArray());
        } catch (DataAccessException redisDown) {
            log.warn("Could not give {} back to the stock gate (Redis unreachable or too slow); the counters stay low "
                    + "until they expire", describe(lines));
        }
    }

    /** Best effort, like giveBack: a marker left behind only means a retry within the TTL takes no units in Redis. */
    private void forget(String requestKey) {
        try {
            redis.delete(requestKey);
        } catch (DataAccessException redisDown) {
            log.warn("Could not delete {} (Redis unreachable or too slow); it expires by itself", requestKey);
        }
    }

    private static List<String> keys(long saleId, List<StockLine> lines) {
        return lines.stream().map(line -> key(saleId, line.saleItemId())).toList();
    }

    /** KEYS for TAKE: the request's marker first, then one counter per line. */
    private static List<String> takeKeys(long saleId, String requestKey, List<StockLine> lines) {
        List<String> keys = new ArrayList<>();
        keys.add(requestKey);
        keys.addAll(keys(saleId, lines));
        return keys;
    }

    /** ARGV for TAKE: the mode, the counter lifetime in ms, then "units wanted, seed" for each line. */
    private Object[] takeArgs(List<StockLine> lines, Map<Long, SaleItem> itemsById, ReservationMode mode) {
        List<String> args = new ArrayList<>();
        args.add(mode.name());
        args.add(String.valueOf(Math.max(1, properties.stockGateTtl().toMillis())));
        for (StockLine line : lines) {
            args.add(String.valueOf(line.quantity()));
            args.add(String.valueOf(itemsById.get(line.saleItemId()).getAvailableStock()));
        }
        return args.toArray();
    }

    /** One more request for this outcome: passed, refused, copy, off or redis_down. */
    private void count(String outcome) {
        meters.counter("flashsale.stock.gate", "outcome", outcome).increment();
    }

    private static List<String> describe(List<StockLine> lines) {
        return lines.stream().map(line -> line.saleItemId() + "x" + line.quantity()).toList(); // e.g. [7x1, 9x2]
    }

    /**
     * What the gate decided for one cart.
     *
     * @param requestKey   the request's marker in Redis (see requestKey)
     * @param letThrough   the lines to try in PostgreSQL, lowest sale item id first
     * @param takenInRedis true when Redis really took these units, so whatever PostgreSQL doesn't keep must be given
     *                     back; false when the gate was off, Redis was down, or this is a copy of a request that
     *                     already took its units (nothing to give back)
     */
    public record Admission(long saleId, String requestKey, List<StockLine> letThrough, boolean takenInRedis) {

        static Admission skipped(long saleId, String requestKey, List<StockLine> lines) {
            return new Admission(saleId, requestKey, lines, false);
        }
    }
}
