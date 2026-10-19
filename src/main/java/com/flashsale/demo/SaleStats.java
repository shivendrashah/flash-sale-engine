package com.flashsale.demo;

import java.util.List;
import java.util.Map;

/**
 * One sale's numbers, live, for the demo page:
 *   per item: the stock, what is left, what is held (waiting for payment), what is sold, and the two
 *   correctness checks of StockAudit (oversold, mismatch), for this sale only;
 *   for the sale: orders, orders notified (the Kafka path: outbox -> relay -> Kafka -> worker), and buyers in
 *   the waiting room's line;
 *   the crowd simulator's counters (CrowdSimulator), when a crowd was sent.
 */
public record SaleStats(long saleId, List<Item> items, long orders, long notified, long inQueue,
                        Map<String, Long> crowd) {

    /**
     * @param held     units in HELD or PAYMENT_PENDING holds
     * @param sold     units in CONFIRMED holds (orders)
     * @param oversold units held or sold beyond the stock. MUST be 0.
     * @param mismatch true when available + held + sold != total (a lost update, see StockAudit)
     */
    public record Item(long saleItemId, String productName, int totalStock, int availableStock, long held, long sold,
                       long oversold, boolean mismatch) {
    }
}
