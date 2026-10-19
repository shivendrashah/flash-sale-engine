package com.flashsale.inventory;

import java.util.List;

import com.flashsale.catalog.SoldOutException;

/**
 * Decides WHAT happens when a cart item can't be filled: give up on the whole cart, or skip that one item.
 * It never touches the database. HOW each stock row is protected is a different question, answered by the
 * StockReserver strategy (row lock, version check, conditional UPDATE, ...).
 *
 *      an exception escaping the transaction rolls back every stock change made so far, so ALL_OR_NOTHING can
 *      never leave half a cart reserved.
 */
public interface ReservationPolicy {

    /** Which ReservationMode this policy implements (ReservationPolicyFactory indexes them by this). */
    ReservationMode mode();

    /**
     * The fewest units a hold made from these lines can contain under this policy. HoldService uses it to refuse a
     * request early when even that can't fit the buyer's purchase limit.
     */
    int fewestUnits(List<StockLine> requested);

    /**
     * One cart line can't be filled (not enough stock).
     * Throw to abandon the WHOLE reservation (the transaction rolls back); return normally to skip just this line.
     */
    void onShortage(StockLine line);

    /**
     * Called after every line was tried, still inside the transaction, with the lines that were taken.
     * The default rule: never create an empty hold. A policy can add its own rules here (e.g. "at least 2 items").
     */
    default void onFinished(List<StockLine> taken, List<StockLine> requested) {
        if (taken.isEmpty()) {
            throw SoldOutException.noneLeft(requested.size()); // HTTP 409 SOLD_OUT; nothing was held
        }
    }
}
