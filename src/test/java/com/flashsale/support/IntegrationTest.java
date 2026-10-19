package com.flashsale.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import com.flashsale.TestcontainersConfiguration;
import com.flashsale.catalog.CreateSaleRequest;
import com.flashsale.catalog.SaleService;
import com.flashsale.catalog.SaleView;
import com.flashsale.hold.CreateHoldCommand;
import com.flashsale.hold.HoldService;
import com.flashsale.hold.HoldView;
import com.flashsale.inventory.StockGate;
import com.flashsale.payment.PayCommand;
import com.flashsale.payment.PaymentProvider;
import com.flashsale.payment.PaymentService;
import com.flashsale.payment.PaymentView;
import com.flashsale.ratelimit.RateLimiter;

/**
 * The base class for tests that need the WHOLE app with a real PostgreSQL and Redis.
 *
 * Every subclass uses EXACTLY the same settings, so Spring builds the app (and starts the containers) once and
 * reuses it for all test classes (Spring's "context caching"). That's why the whole suite takes seconds.
 */
@SpringBootTest(properties = {
        "flashsale.holds.expiry-job-enabled=false",    // tests run the expiry job by hand, at the moment they choose
        "flashsale.payments.recovery-job-enabled=false",   // ...and the payment recovery job too
        "flashsale.outbox.relay-enabled=false",        // ...and the outbox relay (OutboxRelay.publishPending)
        "flashsale.reconciliation.enabled=false",      // ...and the stock reconciliation job (StockReconciler)
        "flashsale.waiting-room.admitter-enabled=false", // ...and the waiting room's admitter (WaitingRoomAdmitter)
        // Spring Boot switches metric exporters off in tests; MonitoringTest checks what Prometheus would scrape
        "management.prometheus.metrics.export.enabled=true",
        "flashsale.notifications.retry-backoff=100ms", // a failing notification reaches the DLT in 0.3 s, not 3 s
        "flashsale.payments.fake-latency=0ms",
        "flashsale.payments.gateway-timeout=200ms",    // tok_timeout / tok_lost give up after 200 ms, not 2 s
        "flashsale.payments.circuit-breaker.wait-in-open-state=500ms",   // an open circuit retries after 0.5 s
        "flashsale.inventory.naive-race-window=50ms",  // makes the NAIVE race happen on every run
        "flashsale.demo.think-time=0ms",               // the demo crowd pays at once...
        "flashsale.demo.queue-patience=3s"             // ...and leaves the line after 3 s (DemoTest)
})
@AutoConfigureMockMvc
@Import({TestcontainersConfiguration.class, TestClockConfiguration.class})
public abstract class IntegrationTest {

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected StringRedisTemplate redis;

    @Autowired
    protected MutableClock clock;

    @Autowired
    protected SaleService saleService;

    @Autowired
    protected HoldService holdService;

    @Autowired
    protected PaymentService paymentService;

    @Autowired
    protected CircuitBreakerRegistry circuitBreakers;   // the payment providers' circuit breakers

    @BeforeEach
    void startFromACleanSlate() {
        jdbc.execute("TRUNCATE notifications, outbox_events, orders, payments, hold_items, holds, user_sale_units, "
                + "sale_items, sales, products RESTART IDENTITY CASCADE");
        flushRedis();
        circuitBreakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
        clock.reset();
    }

    /** FLUSHALL deletes every key (leftover locks, stock gate counters). Also how tests fake a Redis restart. */
    protected void flushRedis() {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    /** A sale that started a minute ago and runs for an hour, with one item. Returns the sale item id. */
    protected long saleItemWithStock(int stock) {
        return liveSaleWithStock(stock).items().getFirst().saleItemId();
    }

    protected SaleView liveSaleWithStock(int stock) {
        Instant now = clock.instant();
        return saleService.createSale(new CreateSaleRequest("Test Sale", now.minus(Duration.ofMinutes(1)),
                now.plus(Duration.ofHours(1)), List.of(iphone(stock))));
    }

    protected static CreateSaleRequest.Item iphone(int stock) {
        return new CreateSaleRequest.Item("iPhone", new BigDecimal("79999.00"), new BigDecimal("59999.00"), stock);
    }

    /**
     * ONE live sale with one item per stock number, for cart tests. Item i costs (i + 1) x 100.00, so totals show
     * which items a hold really contains. Returns the sale item ids, lowest first.
     */
    protected long[] saleItemsWithStock(int... stocks) {
        Instant now = clock.instant();
        List<CreateSaleRequest.Item> items = new ArrayList<>();
        for (int i = 0; i < stocks.length; i++) {
            BigDecimal price = BigDecimal.valueOf(100L * (i + 1)).setScale(2);
            items.add(new CreateSaleRequest.Item("Product " + (i + 1), price, price, stocks[i]));
        }
        SaleView sale = saleService.createSale(new CreateSaleRequest("Cart Sale", now.minus(Duration.ofMinutes(1)),
                now.plus(Duration.ofHours(1)), items));
        return sale.items().stream().mapToLong(SaleView.Item::saleItemId).toArray();
    }

    /**
     * ONE live sale where each buyer may hold or buy at most {@code perUserLimit} units, with one item per stock
     * number (each costs 100.00). Returns the sale item ids, lowest first.
     */
    protected long[] limitedSaleItems(int perUserLimit, int... stocks) {
        Instant now = clock.instant();
        List<CreateSaleRequest.Item> items = new ArrayList<>();
        for (int i = 0; i < stocks.length; i++) {
            BigDecimal price = new BigDecimal("100.00");
            items.add(new CreateSaleRequest.Item("Limited " + (i + 1), price, price, stocks[i]));
        }
        SaleView sale = saleService.createSale(new CreateSaleRequest("Limited Sale",
                now.minus(Duration.ofMinutes(1)), now.plus(Duration.ofHours(1)), items, perUserLimit));
        return sale.items().stream().mapToLong(SaleView.Item::saleItemId).toArray();
    }

    /** Calls HoldService directly, skipping HTTP. Each call gets a fresh Idempotency-Key. */
    protected HoldView holdFor(String userId, long saleItemId, int quantity) {
        return holdService.createHold(holdCommand(userId, UUID.randomUUID().toString(), saleItemId, quantity)).body();
    }

    protected static CreateHoldCommand holdCommand(String userId, String idempotencyKey, long saleItemId,
                                                   int quantity) {
        return new CreateHoldCommand(userId, idempotencyKey, null,
                List.of(new CreateHoldCommand.Item(saleItemId, quantity)));
    }

    protected PaymentView pay(String userId, long holdId, String paymentToken) {
        return paymentService.pay(new PayCommand(userId, holdId, UUID.randomUUID().toString(),
                PaymentProvider.STRIPE, paymentToken)).body();
    }

    protected int availableStock(long saleItemId) {
        return jdbc.queryForObject("SELECT available_stock FROM sale_items WHERE id = ?", Integer.class, saleItemId);
    }

    /** Units held or sold, counted from the hold rows. This doesn't trust the stock column at all. */
    protected int unitsHeldOrSold(long saleItemId) {
        return jdbc.queryForObject("""
                SELECT COALESCE(SUM(hi.quantity), 0)
                  FROM hold_items hi JOIN holds h ON h.id = hi.hold_id
                 WHERE hi.sale_item_id = ? AND h.status IN ('HELD', 'PAYMENT_PENDING', 'CONFIRMED')
                """, Integer.class, saleItemId);
    }

    protected int count(String table) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
    }

    /** The Redis key of a sale item's stock gate counter, e.g. "stock:{1}:2" (StockGate.key). */
    protected String gateKey(long saleItemId) {
        long saleId = jdbc.queryForObject("SELECT sale_id FROM sale_items WHERE id = ?", Long.class, saleItemId);
        return StockGate.key(saleId, saleItemId);
    }

    /** The Redis stock gate's counter for a sale item, or null when Redis has none (nobody asked the gate yet). */
    protected Integer gateCounter(long saleItemId) {
        String value = redis.opsForValue().get(gateKey(saleItemId));
        return value == null ? null : Integer.valueOf(value);
    }

    /** Overwrites a gate counter, e.g. to make Redis "lie" about the stock. */
    protected void setGateCounter(long saleItemId, int units) {
        redis.opsForValue().set(gateKey(saleItemId), String.valueOf(units));
    }

    /**
     * The stock gate's one rule held: every counter Redis has equals PostgreSQL's available_stock, because each unit
     * taken in Redis was also taken in PostgreSQL, or given back. Items with no counter are skipped.
     */
    protected void assertGateMatchesDatabase() {
        for (Map<String, Object> row : jdbc.queryForList("SELECT id, available_stock FROM sale_items ORDER BY id")) {
            long saleItemId = ((Number) row.get("id")).longValue();
            Integer counter = gateCounter(saleItemId);
            if (counter != null) {
                assertThat(counter).as("Redis counter of sale item %d vs PostgreSQL", saleItemId)
                        .isEqualTo(((Number) row.get("available_stock")).intValue());
            }
        }
    }

    /**
     * Empties a buyer's token bucket (RateLimiter), as if they had just sent their whole burst. With the default
     * refill of 5 a second, the next token drips in about 200 ms later.
     */
    protected void emptyBucket(String userId) {
        Long now = redis.execute((RedisCallback<Long>) connection -> connection.serverCommands().time());
        redis.opsForHash().putAll(RateLimiter.key(userId), Map.of("tokens", "0", "at", String.valueOf(now)));
    }

    /**
     * Makes a buyer's bucket look {@code time} older, so the next request refills it as if that much time had
     * passed. The bucket stores when it was last worked out ("at"); moving that back is the same as waiting.
     */
    protected void ageBucket(String userId, Duration time) {
        redis.opsForHash().increment(RateLimiter.key(userId), "at", -time.toMillis());
    }
}
