package com.flashsale.hold;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flashsale.inventory.StockLine;

/**
 * What the API returns for a hold. Jackson turns it into JSON like:
 *   {"holdId": 4, "userId": "alice", "saleId": 2, "status": "HELD", "totalAmount": 59999.00,
 *    "expiresAt": "2026-10-02T18:34:13Z", "items": [{"saleItemId": 2, "quantity": 1, "unitPrice": 59999.00}]}
 * After a PARTIAL cart that skipped something, it also has
 *    "unavailable": [{"saleItemId": 7, "quantity": 1}]
 *
 *       is off), and the API shape doesn't change when the entity does.
 */
public record HoldView(long holdId, String userId, long saleId, HoldStatus status, BigDecimal totalAmount,
                       Instant expiresAt, List<Item> items,
                       @JsonInclude(JsonInclude.Include.NON_EMPTY) List<UnavailableItem> unavailable) {

    public record Item(long saleItemId, int quantity, BigDecimal unitPrice) {
    }

    /** A cart line a PARTIAL hold could not take (not enough stock), with the quantity that was asked for. */
    public record UnavailableItem(long saleItemId, int quantity) {
    }

    /** A hold on its own (GET, cancel, payment): nothing to report as unavailable. */
    public static HoldView of(Hold hold) {
        return of(hold, List.of());
    }

    /** A hold just created (or replayed) from a cart: {@code unavailable} = the lines asked for but not held. */
    public static HoldView of(Hold hold, List<StockLine> unavailable) {
        List<Item> items = hold.getItems().stream()
                .map(item -> new Item(item.getSaleItemId(), item.getQuantity(), item.getUnitPrice()))
                .toList();
        List<UnavailableItem> skipped = unavailable.stream()
                .map(line -> new UnavailableItem(line.saleItemId(), line.quantity()))
                .toList();
        return new HoldView(hold.getId(), hold.getUserId(), hold.getSaleId(), hold.getStatus(),
                hold.getTotalAmount(), hold.getExpiresAt(), items, skipped);
    }
}
