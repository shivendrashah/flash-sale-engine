package com.flashsale.inventory;

import java.util.List;

import org.springframework.stereotype.Component;

/**
 * "Hold what you can": a short item is skipped, the rest is held, and the response lists the skipped items
 * under "unavailable". The hold's total only counts what was actually held. A normal shopping cart.
 * If NOTHING could be held, the inherited onFinished() rule still refuses (409 SOLD_OUT): no empty holds.
 */
@Component
class PartialPolicy implements ReservationPolicy {

    @Override
    public ReservationMode mode() {
        return ReservationMode.PARTIAL;
    }

    /** Lines are kept or skipped whole, and an empty hold is refused, so at least the smallest line is held. */
    @Override
    public int fewestUnits(List<StockLine> requested) {
        return requested.stream().mapToInt(StockLine::quantity).min().orElse(0);
    }

    @Override
    public void onShortage(StockLine line) {
        // Nothing to do: returning normally tells the reserver "skip this line and carry on". HoldService works out
        // which lines were skipped (requested minus taken) and returns them as "unavailable".
    }
}
