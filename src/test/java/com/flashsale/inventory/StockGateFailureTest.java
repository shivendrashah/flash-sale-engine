package com.flashsale.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.math.BigDecimal;
import java.net.SocketException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.flashsale.catalog.SaleItem;

/**
 * What the Redis stock gate does when Redis is unreachable or too slow: it steps aside ("fails open") and
 * lets PostgreSQL decide alone, and a failed give-back never fails the request.
 */
class StockGateFailureTest {

    private static final InventoryProperties SETTINGS = new InventoryProperties(ConcurrencyStrategy.PESSIMISTIC, 10,
            Duration.ofSeconds(5), Duration.ofSeconds(3), Duration.ZERO, true, Duration.ofSeconds(60));

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    @Test
    void ifRedisIsDownTheGateStepsAsideAndPostgresDecidesAlone() {
        StockGate gate = new StockGate(redisThatFails(new RedisConnectionFailureException("Redis is down")),
                SETTINGS, meters);

        StockGate.Admission admission = gate.admit(5L, "5:alice:k1",
                List.of(new StockLine(7L, 1), new StockLine(3L, 2)), itemsInStock(3L, 7L), new AllOrNothingPolicy(),
                null);

        assertThat(admission.letThrough()).as("every line goes on to PostgreSQL, lowest id first")
                .containsExactly(new StockLine(3L, 2), new StockLine(7L, 1));
        assertThat(admission.takenInRedis()).as("Redis took nothing, so nothing will be given back").isFalse();
        assertThat(meters.counter("flashsale.stock.gate", "outcome", "redis_down").count()).isEqualTo(1);
    }

    @Test
    void ifTheConnectionBreaksMidCommandTheGateStepsAsideToo() {
        StockGate gate = new StockGate(redisThatFails(new RedisSystemException("Redis exception",
                new SocketException("Connection reset"))), SETTINGS, meters);

        StockGate.Admission admission = gate.admit(5L, "5:alice:k1", List.of(new StockLine(7L, 1)),
                itemsInStock(7L), new AllOrNothingPolicy(), null);

        assertThat(admission.letThrough()).containsExactly(new StockLine(7L, 1));
        assertThat(admission.takenInRedis()).isFalse();
    }

    @Test
    void ifRedisIsTooSlowTheGateStepsAsideToo() {
        StockGate gate = new StockGate(redisThatFails(new QueryTimeoutException("Redis command timed out")),
                SETTINGS, meters);

        StockGate.Admission admission = gate.admit(5L, "5:alice:k1", List.of(new StockLine(7L, 1)),
                itemsInStock(7L), new PartialPolicy(), null);

        assertThat(admission.letThrough()).containsExactly(new StockLine(7L, 1));
        assertThat(admission.takenInRedis()).isFalse();
    }

    @Test
    void ifRedisIsDownWhileGivingBackTheRequestStillSucceeds() {
        StockGate gate = new StockGate(redisThatFails(new RedisConnectionFailureException("Redis is down")),
                SETTINGS, meters);
        StockGate.Admission tookOneUnit = new StockGate.Admission(5L, "stock:{5}:request:5:alice:k1",
                List.of(new StockLine(7L, 1)), true);

        // No exception: the database work already happened (a hold saved, cancelled or expired), so failing now would
        // only hide a success. The counter stays a little low until it expires.
        assertThatCode(() -> gate.giveBackUnused(tookOneUnit, List.of())).doesNotThrowAnyException();
        assertThatCode(() -> gate.giveBack(List.of(new HeldStock(5L, List.of(new StockLine(7L, 1))))))
                .doesNotThrowAnyException();
    }

    @Test
    void withTheGateSwitchedOffRedisIsNeverCalled() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        StockGate gate = new StockGate(redis, SETTINGS, meters);

        StockGate.Admission admission = gate.admit(5L, "5:alice:k1", List.of(new StockLine(7L, 1)),
                itemsInStock(7L), new AllOrNothingPolicy(), false);   // false = the header "X-Stock-Gate: off"

        assertThat(admission.takenInRedis()).isFalse();
        verifyNoInteractions(redis);
    }

    /** A fake StringRedisTemplate where EVERY method throws the given exception. */
    private static StringRedisTemplate redisThatFails(RuntimeException failure) {
        return mock(StringRedisTemplate.class, invocation -> {
            throw failure;
        });
    }

    /** The sale items as HoldService would pass them in (only available_stock matters here: it's the seed). */
    private static Map<Long, SaleItem> itemsInStock(Long... saleItemIds) {
        return Arrays.stream(saleItemIds).collect(Collectors.toMap(
                id -> id, id -> new SaleItem(5L, id, new BigDecimal("100.00"), 10)));
    }
}
