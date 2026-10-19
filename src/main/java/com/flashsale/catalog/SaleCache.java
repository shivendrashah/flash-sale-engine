package com.flashsale.catalog;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * The sale page (GET /api/sales/{id}) is the most-read thing in a flash sale: everyone refreshes it while
 * waiting for the start. This class keeps a copy of each sale page in Redis for one second ("cache-aside"):
 *   in Redis:      answer from Redis. PostgreSQL is not touched.
 *   not in Redis:  read PostgreSQL (SaleService.getSale), store the answer for 1 s, return it.
 * On top of that, only ONE request per sale (per app instance) reads PostgreSQL at a time. The others that
 * miss at the same moment wait for its answer instead of all running the same queries ("single flight").
 *   10,000 page views a second on one instance  ->  about 1 sale read a second in PostgreSQL
 *
 * Why not Spring's @Cacheable? It can do this (even the single flight, with sync = true), but it hides every step
 * behind an annotation, it needs extra setup to fail open and to store records as JSON, and it can't recompute
 * "active" on the way out. Written by hand, each step is visible and tested. shows the @Cacheable
 * version for comparison.
 */
@Component
public class SaleCache {

    private static final Logger log = LoggerFactory.getLogger(SaleCache.class);

    private final SaleService saleService;
    private final StringRedisTemplate redis;
    private final JsonMapper json;
    private final SaleCacheProperties properties;
    private final Clock clock;
    private final MeterRegistry meters;

    private final ConcurrentHashMap<Long, CompletableFuture<SaleView>> loading = new ConcurrentHashMap<>();

    public SaleCache(SaleService saleService, StringRedisTemplate redis, JsonMapper json,
                     SaleCacheProperties properties, Clock clock, MeterRegistry meters) {
        this.saleService = saleService;
        this.redis = redis;
        this.json = json;
        this.properties = properties;
        this.clock = clock;
        this.meters = meters;
    }

    /** The Redis key of a sale page: "cache:sale:42". */
    public static String key(long saleId) {
        return "cache:sale:" + saleId;
    }

    /**
     * Never fails because of Redis: if Redis is down, the page comes from PostgreSQL.
     * @throws com.flashsale.common.error.NotFoundException if there is no such sale (404).
     *         A missing sale is not cached.
     */
    public SaleView getSale(long saleId) {
        if (!properties.enabled()) {
            return saleService.getSale(saleId);
        }
        SaleView view;
        try {
            view = read(saleId);
        } catch (DataAccessException redisDown) {
            log.warn("Sale cache skipped for sale {} (Redis unreachable or too slow: {})", saleId,
                    redisDown.getMessage());
            count("redis_down");
            return loadOnce(saleId, false).at(clock.instant());
        }
        if (view != null) {
            count("hit");
        } else {
            view = loadOnce(saleId, true);
        }
        return view.at(clock.instant());
    }

    /**
     * stores it in Redis. If one is, wait for its answer instead.
     *
     *
     * @param redisUp false when Redis just failed: then skip it entirely
     */
    private SaleView loadOnce(long saleId, boolean redisUp) {
        CompletableFuture<SaleView> mine = new CompletableFuture<>();
        CompletableFuture<SaleView> running = loading.putIfAbsent(saleId, mine);
        if (running != null) {
            count("shared");                 // another request is loading this sale: use its answer
            return waitFor(running);
        }
        try {
            SaleView view = redisUp ? readQuietly(saleId) : null;
            if (view != null) {
                count("hit");
            } else {
                count("miss");               // this request reads PostgreSQL
                view = saleService.getSale(saleId);
                if (redisUp) {
                    write(saleId, view);
                }
            }
            mine.complete(view);
            return view;
        } catch (RuntimeException e) {
            mine.completeExceptionally(e);
            throw e;
        } finally {
            loading.remove(saleId, mine);
        }
    }

    private static SaleView waitFor(CompletableFuture<SaleView> running) {
        try {
            return running.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw e;
        }
    }

    /**
     * The cached copy, or null when Redis has none (never stored, or its TTL ran out) or holds one we can't read.
     * @throws DataAccessException when Redis is unreachable or too slow
     */
    private SaleView read(long saleId) {
        String text = redis.opsForValue().get(key(saleId));
        if (text == null) {
            return null;
        }
        try {
            return json.readValue(text, SaleView.class);
        } catch (JacksonException unreadable) {
            log.warn("Cached sale {} is unreadable, reloading it: {}", saleId, unreadable.getMessage());
            return null;
        }
    }

    /** The same, but a Redis failure counts as "not there" (used for the re-check, after the first read worked). */
    private SaleView readQuietly(long saleId) {
        try {
            return read(saleId);
        } catch (DataAccessException redisDown) {
            count("redis_down");
            return null;
        }
    }

    /**
     * There is no "invalidate on change": stock changes on every purchase, so the copy simply expires (see HLD 9.11).
     */
    private void write(long saleId, SaleView view) {
        try {
            redis.opsForValue().set(key(saleId), json.writeValueAsString(view), properties.ttl());
        } catch (DataAccessException redisDown) {
            // The page is already loaded; failing to store it only means the next view loads it again
            log.warn("Sale {} not cached (Redis unreachable or too slow: {})", saleId, redisDown.getMessage());
            count("redis_down");
        }
    }

    /**
     * One more page view for this outcome: hit (from Redis), miss (read PostgreSQL) or shared (waited for another
     * request's read). redis_down counts Redis failures, on top of the page view's own outcome.
     */
    private void count(String outcome) {
        meters.counter("flashsale.sale.cache", "outcome", outcome).increment();
    }
}
