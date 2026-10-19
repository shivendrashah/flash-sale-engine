package com.flashsale.demo;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.flashsale.catalog.SaleService;
import com.flashsale.waitingroom.WaitingRoom;

/**
 * Counts one sale's stock, holds, orders and notifications, straight from PostgreSQL, plus the waiting room's
 * line from Redis and the crowd's counters. The demo page asks every second.
 */
@Service
public class SaleStatsService {

    private static final String ITEMS = """
            SELECT si.id, p.name, si.total_stock, si.available_stock,
                   COALESCE(SUM(hi.quantity) FILTER (WHERE h.status IN ('HELD', 'PAYMENT_PENDING')), 0) AS held,
                   COALESCE(SUM(hi.quantity) FILTER (WHERE h.status = 'CONFIRMED'), 0)                AS sold
              FROM sale_items si
              JOIN products p ON p.id = si.product_id
              LEFT JOIN hold_items hi ON hi.sale_item_id = si.id
              LEFT JOIN holds h ON h.id = hi.hold_id
             WHERE si.sale_id = ?
             GROUP BY si.id, p.name
             ORDER BY si.id
            """;

    private static final String ORDERS = """
            SELECT COUNT(*) FROM orders o JOIN holds h ON h.id = o.hold_id WHERE h.sale_id = ?
            """;

    private static final String NOTIFIED = """
            SELECT COUNT(*) FROM notifications n JOIN orders o ON o.id = n.order_id JOIN holds h ON h.id = o.hold_id
             WHERE h.sale_id = ?
            """;

    private final JdbcTemplate jdbc;
    private final SaleService saleService;
    private final WaitingRoom waitingRoom;
    private final CrowdSimulator crowd;

    public SaleStatsService(JdbcTemplate jdbc, SaleService saleService, WaitingRoom waitingRoom,
                            CrowdSimulator crowd) {
        this.jdbc = jdbc;
        this.saleService = saleService;
        this.waitingRoom = waitingRoom;
        this.crowd = crowd;
    }

    public SaleStats stats(long saleId) {
        saleService.getSale(saleId);   // 404 for a sale that doesn't exist
        List<SaleStats.Item> items = jdbc.query(ITEMS, (row, n) -> {
            int total = row.getInt("total_stock");
            int available = row.getInt("available_stock");
            long held = row.getLong("held");
            long sold = row.getLong("sold");
            return new SaleStats.Item(row.getLong("id"), row.getString("name"), total, available, held, sold,
                    Math.max(held + sold - total, 0), available + held + sold != total);
        }, saleId);
        long orders = jdbc.queryForObject(ORDERS, Long.class, saleId);
        long notified = jdbc.queryForObject(NOTIFIED, Long.class, saleId);
        return new SaleStats(saleId, items, orders, notified, inQueue(saleId), crowd.progress(saleId));
    }

    /** Buyers in the waiting room's line; -1 if Redis can't say (the page shows "?"). */
    private long inQueue(long saleId) {
        try {
            return waitingRoom.queueLength(saleId);
        } catch (RuntimeException e) {
            return -1;
        }
    }
}
