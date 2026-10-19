package com.flashsale.hold.web;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import com.flashsale.hold.CreateHoldCommand;
import com.flashsale.inventory.ConcurrencyStrategy;
import com.flashsale.inventory.ReservationMode;

/**
 * The JSON body of POST /api/holds, a cart of 1 to 10 items plus what to do if some are sold out:
 *   {"items": [{"saleItemId": 3, "quantity": 1}, {"saleItemId": 7, "quantity": 2}], "mode": "PARTIAL"}
 * "mode" is optional (ALL_OR_NOTHING when missing), so a Phase 1 one-item request still works unchanged.
 */
record CreateHoldRequest(
        @NotNull @Size(min = 1, max = 10, message = "a hold can have 1 to 10 items")
        List<@NotNull @Valid Item> items,
        ReservationMode mode) {

    CreateHoldRequest {
        if (mode == null) {
            mode = ReservationMode.ALL_OR_NOTHING; // the safe default, and exactly what Phase 1 did
        }
    }

    record Item(@NotNull @Positive Long saleItemId, @Min(1) @Max(10) int quantity) {
    }

    /** Bean Validation treats this like a field named "eachItemListedOnce" that must be true (see isWindowValid). */
    @AssertTrue(message = "each saleItemId may appear only once")
    public boolean isEachItemListedOnce() {
        if (items == null) {
            return true; // @NotNull above reports the missing list; don't report it twice
        }
        Set<Long> seen = new HashSet<>();
        for (Item item : items) {
            if (item != null && item.saleItemId() != null && !seen.add(item.saleItemId())) {
                return false;
            }
        }
        return true;
    }

    /** Combine the body with the headers into one command for HoldService. */
    CreateHoldCommand toCommand(String userId, String idempotencyKey, ConcurrencyStrategy strategy, Boolean stockGate) {
        return new CreateHoldCommand(userId, idempotencyKey, strategy, stockGate, mode,
                items.stream().map(item -> new CreateHoldCommand.Item(item.saleItemId(), item.quantity())).toList());
    }
}
