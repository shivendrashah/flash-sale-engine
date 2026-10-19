package com.flashsale.hold;

import java.util.List;

import com.flashsale.common.RequestFingerprint;
import com.flashsale.inventory.ConcurrencyStrategy;
import com.flashsale.inventory.ReservationMode;
import com.flashsale.inventory.StockLine;

/**
 * Everything HoldService needs to create a hold, in one immutable object: who, which items (a cart of 1 to
 * 10 lines), what to do if some are sold out (mode), the idempotency key, and (optionally) which concurrency
 * strategy and whether to use the Redis stock gate.
 *
 * @param strategy  which concurrency strategy to use, or null for the configured default
 * @param stockGate true/false to switch the Redis stock gate on/off for this request (X-Stock-Gate), or null for
 *                  the configured default (flashsale.inventory.stock-gate-enabled)
 * @param mode      what to do when some items are short; null means ALL_OR_NOTHING
 */
public record CreateHoldCommand(String userId, String idempotencyKey, ConcurrencyStrategy strategy,
                                Boolean stockGate, ReservationMode mode, List<Item> items) {

    public CreateHoldCommand {
        if (mode == null) {
            mode = ReservationMode.ALL_OR_NOTHING;
        }
    }

    /** Without the gate switch: the Redis stock gate follows application.yml. */
    public CreateHoldCommand(String userId, String idempotencyKey, ConcurrencyStrategy strategy,
                             ReservationMode mode, List<Item> items) {
        this(userId, idempotencyKey, strategy, null, mode, items);
    }

    /** The Phase 1 shape (no mode), still used by older tests: means ALL_OR_NOTHING. */
    public CreateHoldCommand(String userId, String idempotencyKey, ConcurrencyStrategy strategy, List<Item> items) {
        this(userId, idempotencyKey, strategy, null, ReservationMode.ALL_OR_NOTHING, items);
    }

    /** A "wither": a copy of this record with one value changed (records are immutable). Used by tests. */
    public CreateHoldCommand withStockGate(boolean on) {
        return new CreateHoldCommand(userId, idempotencyKey, strategy, on, mode, items);
    }

    public record Item(long saleItemId, int quantity) {
    }

    /**
     * Who is asking, and which request: the user and the Idempotency-Key as one string, e.g. "5:alice:double-click".
     * The user id's length comes first so the parts can't blur together: user "a:b" + key "c" gives "3:a:b:c",
     * user "a" + key "b:c" gives "1:a:b:c". The Redis stock gate marks a request with it (StockGate.requestKey).
     */
    public String requestId() {
        return userId.length() + ":" + userId + ":" + idempotencyKey;
    }

    /** The cart as the inventory package's neutral StockLine records. */
    public List<StockLine> stockLines() {
        return items.stream().map(item -> new StockLine(item.saleItemId(), item.quantity())).toList();
    }

    /**
     * What makes two requests "the same request": the same items (in any order) and the same mode. The strategy and
     * the stock gate switch are test knobs, so they are left out on purpose. Hashed text example: "PARTIAL|[1x1, 3x2]".
     */
    public String fingerprint() {
        return RequestFingerprint.of(mode, items.stream().map(item -> item.saleItemId() + "x" + item.quantity())
                .sorted()
                .toList());
    }
}
