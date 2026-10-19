package com.flashsale.inventory;

import java.util.List;

import org.springframework.stereotype.Component;

import com.flashsale.catalog.SoldOutException;

/**
 * "Every item, or nothing": the first short item cancels the whole cart. For bundles, combo offers, or seats
 * that must be together. Also the default when a request doesn't send "mode".
 */
@Component
class AllOrNothingPolicy implements ReservationPolicy {

    @Override
    public ReservationMode mode() {
        return ReservationMode.ALL_OR_NOTHING;
    }

    /** All or nothing: a hold has every line, so it always has every unit asked for. */
    @Override
    public int fewestUnits(List<StockLine> requested) {
        return requested.stream().mapToInt(StockLine::quantity).sum();
    }

    @Override
    public void onShortage(StockLine line) {
        throw new SoldOutException(line.saleItemId(), line.quantity()); // HTTP 409 SOLD_OUT, names the short item
    }
}
