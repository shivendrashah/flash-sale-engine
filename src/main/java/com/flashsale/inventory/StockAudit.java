package com.flashsale.inventory;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Two gauges that recount the stock from the hold rows, ignoring available_stock, every time Prometheus asks:
 *   flashsale.stock.oversold   units held or sold beyond an item's total stock, over all items
 *                              MUST be 0. Anything else means the system sold what it didn't have.
 *   flashsale.stock.mismatch   items whose available_stock + units held or sold != total stock
 *                              a lost update (the NAIVE strategy shows it), or a bug that moved stock
 *                              without a hold (or a hold without stock)
 * The tests check the same two things after every concurrency test (OversellTest, CartTest); these gauges
 * check them on the running system, on the Grafana dashboard.
 */
@Component
public class StockAudit {

    private static final String AUDIT = """
            SELECT COALESCE(SUM(GREATEST(taken - total_stock, 0)), 0)                AS oversold,
                   COUNT(*) FILTER (WHERE available_stock + taken <> total_stock)     AS mismatch
              FROM (SELECT si.id, si.total_stock, si.available_stock,
                           COALESCE(SUM(hi.quantity) FILTER (
                               WHERE h.status IN ('HELD', 'PAYMENT_PENDING', 'CONFIRMED')), 0) AS taken
                      FROM sale_items si
                      LEFT JOIN hold_items hi ON hi.sale_item_id = si.id
                      LEFT JOIN holds h ON h.id = hi.hold_id
                     GROUP BY si.id) per_item
            """;

    private final JdbcTemplate jdbc;

    public StockAudit(JdbcTemplate jdbc, MeterRegistry meters) {
        this.jdbc = jdbc;
        Gauge.builder("flashsale.stock.oversold", this, audit -> audit.run().oversold())
                .description("Units held or sold beyond the stock, counted from the hold rows. Must be 0.")
                .register(meters);
        Gauge.builder("flashsale.stock.mismatch", this, audit -> audit.run().mismatch())
                .description("Sale items whose available_stock disagrees with their holds (lost updates)")
                .register(meters);
    }

    /** Recounts every sale item. */
    public Result run() {
        return jdbc.queryForObject(AUDIT, (row, n) -> new Result(row.getLong("oversold"), row.getLong("mismatch")));
    }

    /** @param oversold units taken beyond the stock; @param mismatch items whose numbers don't add up */
    public record Result(long oversold, long mismatch) {
    }
}
