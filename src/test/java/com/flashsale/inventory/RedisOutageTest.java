package com.flashsale.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import com.flashsale.catalog.SoldOutException;
import com.flashsale.hold.CreateHoldCommand;
import com.flashsale.hold.HoldStatus;
import com.flashsale.hold.HoldView;
import com.flashsale.support.IntegrationTest;

/**
 * Stops this class's own Redis container in the middle of a sale and checks what buyers see: the stock gate
 * steps aside and PostgreSQL keeps selling correctly, while REDIS_LOCK refuses (503) rather than sell unsafely.
 * The rate limiter steps aside too: an HTTP request goes through, uncounted. The sale page comes from
 * PostgreSQL instead of its Redis copy.
 *
 * Why this test exists: the first version of the gate (and REDIS_LOCK since Phase 1) caught only
 * RedisConnectionFailureException and QueryTimeoutException. When the real Redis container was stopped, Spring threw
 * RedisSystemException ("Connection reset") instead, and every buyer got a 500. The mocked unit tests couldn't see
 * that; only stopping a real Redis could.
 */
@TestPropertySource(properties = "spring.data.redis.timeout=500ms")
class RedisOutageTest extends IntegrationTest {

    @Autowired
    GenericContainer<?> redisContainer;

    @Autowired
    MockMvc mvc;

    @Autowired
    StockReconciler reconciler;

    @Test
    void whenRedisGoesAwayBuyersAreStillServedCorrectly() throws Exception {
        long saleItemId = saleItemWithStock(2);
        HoldView alice = holdFor("alice", saleItemId, 1);           // through the gate: Redis 1, PostgreSQL 1
        assertThat(gateCounter(saleItemId)).isEqualTo(1);

        DockerClientFactory.instance().client().stopContainerCmd(redisContainer.getContainerId()).exec();

        // The gate steps aside, and PostgreSQL alone sells the last unit...
        HoldView bob = holdFor("bob", saleItemId, 1);
        assertThat(bob.status()).isEqualTo(HoldStatus.HELD);
        // ...and turns the next buyer away itself
        assertThatThrownBy(() -> holdFor("carol", saleItemId, 1)).isInstanceOf(SoldOutException.class);
        // A cancel still works: the unit is back in PostgreSQL; handing it back to Redis fails quietly
        holdService.cancelHold("alice", alice.holdId());
        assertThat(availableStock(saleItemId)).isEqualTo(1);
        // REDIS_LOCK guards correctness, so without Redis it refuses: 503 STOCK_BUSY, nothing sold
        assertThatThrownBy(() -> holdService.createHold(new CreateHoldCommand("dave", "dave-1",
                ConcurrencyStrategy.REDIS_LOCK, List.of(new CreateHoldCommand.Item(saleItemId, 1)))))
                .isInstanceOf(StockBusyException.class);
        assertThat(unitsHeldOrSold(saleItemId)).as("only bob's unit is held").isEqualTo(1);
        // Over HTTP, the rate limiter can't reach its bucket either: it lets erin through, uncounted (no
        // X-RateLimit headers), and PostgreSQL sells her the unit alice gave back
        mvc.perform(post("/api/holds")
                        .header("X-User-Id", "erin")
                        .header("Idempotency-Key", "erin-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"items": [{"saleItemId": %d, "quantity": 1}]}
                                """.formatted(saleItemId)))
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("X-RateLimit-Remaining"));
        assertThat(availableStock(saleItemId)).isZero();
        assertThat(availableStock(saleItemId) + unitsHeldOrSold(saleItemId)).isEqualTo(2);
        // The sale page can't use its Redis copy either: it comes straight from PostgreSQL, and is up to date
        mvc.perform(get("/api/sales/{saleId}", 1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].availableStock").value(0));
        // The reconciliation job has nothing to compare against: it reports Redis as down and carries on
        assertThat(reconciler.reconcile()).isEqualTo(new StockReconciler.Result(0, 0, 0));
    }
}
