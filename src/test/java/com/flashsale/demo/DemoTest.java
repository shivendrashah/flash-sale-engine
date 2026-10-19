package com.flashsale.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.flashsale.catalog.CreateSaleRequest;
import com.flashsale.common.Pause;
import com.flashsale.support.IntegrationTest;
import com.flashsale.waitingroom.WaitingRoomAdmitter;

/**
 * What the React frontend relies on:
 *   GET /api/sales lists the newest sales first (the shop's front page);
 *   the stats endpoint counts held, sold, orders and oversold for one sale (the demo console);
 *   a crowd of fake buyers never oversells, and every buyer ends with exactly one outcome;
 *   through a waiting room, the crowd queues and only admitted buyers buy;
 *   a crowd for a sale far in the future, or a crowd of 0, is refused.
 */
class DemoTest extends IntegrationTest {

    @Autowired
    MockMvc mvc;

    @Autowired
    SaleStatsService stats;

    @Autowired
    CrowdSimulator crowd;

    @Autowired
    WaitingRoomAdmitter admitter;

    @Test
    void theShopListsTheNewestSalesFirst() throws Exception {
        liveSaleWithStock(1);
        liveSaleWithStock(2);
        long newest = liveSaleWithStock(3).id();

        mvc.perform(get("/api/sales"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].id").value(newest))
                .andExpect(jsonPath("$[0].items[0].availableStock").value(3));
    }

    @Test
    void statsCountHeldSoldAndOrdersForOneSale() throws Exception {
        long saleItemId = saleItemWithStock(10);
        long saleId = saleIdOf(saleItemId);
        holdFor("alice", saleItemId, 2);
        pay("bob", holdFor("bob", saleItemId, 1).holdId(), "tok_success");
        saleItemWithStock(5);   // another sale: not counted

        mvc.perform(get("/api/admin/sales/{saleId}/stats", saleId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].availableStock").value(7))
                .andExpect(jsonPath("$.items[0].held").value(2))
                .andExpect(jsonPath("$.items[0].sold").value(1))
                .andExpect(jsonPath("$.items[0].oversold").value(0))
                .andExpect(jsonPath("$.items[0].mismatch").value(false))
                .andExpect(jsonPath("$.orders").value(1))
                .andExpect(jsonPath("$.notified").value(0))   // the outbox relay is off in tests
                .andExpect(jsonPath("$.inQueue").value(0));
        mvc.perform(get("/api/admin/sales/{saleId}/stats", 999)).andExpect(status().isNotFound());
    }

    @Test
    void aCrowdNeverOversellsAndEveryBuyerEndsOnce() {
        long saleItemId = saleItemWithStock(10);
        long saleId = saleIdOf(saleItemId);

        crowd.send(saleId, new CrowdRequest(60, 0));   // 60 buyers at once for 10 units
        Map<String, Long> counts = waitFor(saleId, c -> c.get("done") == 60);

        SaleStats.Item item = stats.stats(saleId).items().getFirst();
        assertThat(item.oversold()).isZero();
        assertThat(item.mismatch()).isFalse();
        assertThat(item.held()).as("no think time: everyone paid or cancelled").isZero();
        assertThat(item.sold()).isEqualTo(counts.get("paid")).isLessThanOrEqualTo(10);
        assertThat(counts.get("held")).isEqualTo(counts.get("paid") + counts.get("cancelled"));
        assertThat(counts.get("held") + counts.get("soldOut") + counts.get("busy"))
                .as("each buyer ends exactly one way").isEqualTo(60);
        assertThat(counts.get("failed")).isZero();
        assertThat(item.sold() + item.availableStock()).as("cancelled units went back on sale").isEqualTo(10);
    }

    @Test
    void throughTheWaitingRoomOnlyAdmittedBuyersBuy() {
        Instant now = clock.instant();
        long saleId = saleService.createSale(new CreateSaleRequest("Queued", now.minus(Duration.ofMinutes(1)),
                now.plus(Duration.ofHours(1)), List.of(iphone(100)), null, 4)).id();

        crowd.send(saleId, new CrowdRequest(10, 0));
        waitUntil(() -> stats.stats(saleId).inQueue() == 10);   // all 10 in line, nobody admitted yet
        assertThat(stats.stats(saleId).items().getFirst().availableStock()).isEqualTo(100);

        assertThat(admitter.admitNext()).isEqualTo(4);          // one second's worth (the admitter job is off)
        Map<String, Long> counts = waitFor(saleId, c -> c.get("done") == 10);   // the other 6 give up after 3 s

        assertThat(counts.get("held")).isEqualTo(4);
        assertThat(counts.get("notAdmitted")).isEqualTo(6);
        assertThat(stats.stats(saleId).items().getFirst().sold() + stats.stats(saleId).items().getFirst().held())
                .isEqualTo(counts.get("paid"));
    }

    @Test
    void aCrowdTooEarlyOrOfNobodyIsRefused() throws Exception {
        Instant now = clock.instant();
        long later = saleService.createSale(new CreateSaleRequest("Tonight", now.plus(Duration.ofHours(5)),
                now.plus(Duration.ofHours(6)), List.of(iphone(10)))).id();

        mvc.perform(post("/api/admin/sales/{saleId}/crowd", later).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"buyers\": 10, \"seconds\": 0}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("CROWD_TOO_EARLY"));
        mvc.perform(post("/api/admin/sales/{saleId}/crowd", later).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"buyers\": 0, \"seconds\": 0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
        assertThat(crowd.progress(later)).as("nothing was sent").isEmpty();
    }

    /** Polls the crowd's counters until {@code done} holds (at most 20 s). */
    private Map<String, Long> waitFor(long saleId, Predicate<Map<String, Long>> done) {
        waitUntil(() -> !crowd.progress(saleId).isEmpty() && done.test(crowd.progress(saleId)));
        return crowd.progress(saleId);
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition) {
        long giveUpAt = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime() - giveUpAt).as("still waiting after 20 s").isNegative();
            Pause.of(Duration.ofMillis(50));
        }
    }

    private long saleIdOf(long saleItemId) {
        return jdbc.queryForObject("SELECT sale_id FROM sale_items WHERE id = ?", Long.class, saleItemId);
    }
}
