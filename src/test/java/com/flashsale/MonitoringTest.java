package com.flashsale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import com.flashsale.hold.CreateHoldCommand;
import com.flashsale.inventory.ConcurrencyStrategy;
import com.flashsale.inventory.ReservationMode;
import com.flashsale.inventory.StockAudit;
import com.flashsale.support.Concurrently;
import com.flashsale.support.IntegrationTest;

/**
 * What Prometheus and the Grafana dashboard rely on:
 *   /actuator/prometheus serves our metrics, request-time buckets (for p99), the connection pool, and the
 *   instance's name as a tag;
 *   the oversell gauges read 0 after a correct sale, and catch a real oversell: the NAIVE strategy's lost
 *   update, with 20 buyers racing for 1 unit.
 */
class MonitoringTest extends IntegrationTest {

    @Autowired
    MockMvc mvc;

    @Autowired
    StockAudit audit;

    @Test
    void prometheusGetsEverythingTheDashboardShows() throws Exception {
        long saleItemId = saleItemWithStock(5);
        holdFor("alice", saleItemId, 1);
        mvc.perform(get("/api/sales/{saleId}", 1)).andExpect(status().isOk());   // one HTTP request to measure

        String scrape = mvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(scrape)
                .contains("flashsale_stock_oversold{")                    // the correctness gauges
                .contains("flashsale_stock_mismatch{")
                .contains("flashsale_stock_gate_total{")                  // our counters, with _total added
                .contains("http_server_requests_seconds_bucket{")         // buckets, so Prometheus can do p99
                .contains("hikaricp_connections_active{")                 // the connection pool
                .contains("application=\"flash-sale-engine\"");          // the instance's name as a tag
    }

    @Test
    void afterACorrectSaleNothingIsOversold() {
        long saleItemId = saleItemWithStock(3);
        List<Concurrently.Outcome<Object>> buyers = Concurrently.run(20, i -> holdFor("buyer-" + i, saleItemId, 1));

        assertThat(Concurrently.successes(buyers)).isEqualTo(3);
        assertThat(audit.run()).isEqualTo(new StockAudit.Result(0, 0));
    }

    // The gauges are only worth having if they catch the real bug: NAIVE reads "1 left", waits, then writes.
    @Test
    void theAuditCatchesTheNaiveStrategysOversell() {
        long saleItemId = saleItemWithStock(1);

        List<Concurrently.Outcome<Object>> buyers = Concurrently.run(20, i -> holdService.createHold(
                // stockGate = false: with Redis's gate in front, even NAIVE looks safe (OversellTest does the same)
                new CreateHoldCommand("buyer-" + i, UUID.randomUUID().toString(), ConcurrencyStrategy.NAIVE, false,
                        ReservationMode.ALL_OR_NOTHING, List.of(new CreateHoldCommand.Item(saleItemId, 1)))));

        long winners = Concurrently.successes(buyers);
        assertThat(winners).as("NAIVE sells the last unit more than once").isGreaterThan(1);
        StockAudit.Result result = audit.run();
        assertThat(result.oversold()).as("units sold that didn't exist").isEqualTo(winners - 1);
        assertThat(result.mismatch()).as("available_stock no longer adds up").isEqualTo(1);
    }
}
