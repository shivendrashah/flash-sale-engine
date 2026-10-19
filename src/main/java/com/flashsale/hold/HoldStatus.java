package com.flashsale.hold;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The lifecycle of a hold, as a state machine. The ALLOWED_MOVES table below is the ONLY place that says
 * which status changes are allowed.
 *
 * <pre>
 *            pay starts               payment succeeds
 *   HELD ─────────────► PAYMENT_PENDING ──────────────► CONFIRMED
 *    │  ◄─────────────────────┘
 *    │    payment declined (try again)
 *    ├──────► EXPIRED     (time ran out; stock goes back)
 *    └──────► CANCELLED   (user gave up; stock goes back)
 * </pre>
 */
public enum HoldStatus {

    /** Stock is set aside for the user; waiting for payment. */
    HELD,
    /** A payment attempt is running. The expiry job must leave this hold alone. */
    PAYMENT_PENDING,
    /** Paid. The stock is sold for good. */
    CONFIRMED,
    /** Time ran out before payment. The stock went back on sale. */
    EXPIRED,
    /** The user cancelled. The stock went back on sale. */
    CANCELLED;

    private static final Map<HoldStatus, Set<HoldStatus>> ALLOWED_MOVES = Map.of(
            HELD, EnumSet.of(PAYMENT_PENDING, EXPIRED, CANCELLED),
            PAYMENT_PENDING, EnumSet.of(CONFIRMED, HELD),
            CONFIRMED, EnumSet.noneOf(HoldStatus.class),
            EXPIRED, EnumSet.noneOf(HoldStatus.class),
            CANCELLED, EnumSet.noneOf(HoldStatus.class));

    public boolean canMoveTo(HoldStatus next) {
        return ALLOWED_MOVES.get(this).contains(next);
    }

    /** Final states never change again. */
    public boolean isFinal() {
        return ALLOWED_MOVES.get(this).isEmpty();
    }
}
