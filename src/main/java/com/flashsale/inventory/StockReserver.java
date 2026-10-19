package com.flashsale.inventory;

import java.util.List;
import java.util.function.Function;

import com.flashsale.catalog.SoldOutException;

/**
 * The job every concurrency strategy does: "take the stock for these cart lines, safely, even if 1,000 buyers
 * do it at the same moment".
 *
 *                  can't be protected one variable at a time; it needs one transaction around both.
 * The important rules every implementation follows:
 *   1. ONE database transaction for the whole cart: every stock change AND {@code saveHold}. Then a hold is never
 *      saved without its stock, stock is never taken without a hold, and a rollback undoes the whole cart.
 *   2. Visit the lines in StockLine.inIdOrder(...) order, so every transaction locks rows in the same order
 *      (no deadlocks between two carts that share items).
 *   3. A short line goes to {@code policy.onShortage(line)}: it throws (give up, roll back) or returns (skip it).
 *
 *        a T, written as a lambda: taken -> {...}. HoldService passes "save the hold" this way; the strategy calls
 *        saveHold.apply(taken) at the right moment, inside its transaction. This is called a "callback".
 */
public interface StockReserver {

    /** Which ConcurrencyStrategy this implementation handles (StockReserverFactory indexes them by this). */
    ConcurrencyStrategy strategy();

    /**
     * Takes the stock for {@code lines} (any order; visited in id order), asking {@code policy} what to do with a short
     * line, then runs {@code saveHold} with the lines it took, before committing.
     *
     * @return whatever {@code saveHold} returned
     * @throws SoldOutException   if the policy gives up on a short line (or, by default, if nothing could be taken)
     * @throws StockBusyException if too many buyers are on these items right now; the client should retry
     */
    <T> T reserve(List<StockLine> lines, ReservationPolicy policy, Function<List<StockLine>, T> saveHold);
}
