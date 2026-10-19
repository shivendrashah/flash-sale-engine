package com.flashsale.inventory;

/**
 * The five ways this project can protect the stock number when many buyers change it at the same moment.
 * Each value has one StockReserver class that implements it.
 *
 *        automatically; an unknown value like "FASTEST" becomes HTTP 400.
 */
public enum ConcurrencyStrategy {

    /** BROKEN on purpose: read, check in Java, write back. Reproduces overselling. Never use it for real. */
    NAIVE,

    /** Lock the row (SELECT ... FOR UPDATE). Other buyers queue up and wait their turn. */
    PESSIMISTIC,

    /** No lock. Detect a clash with the version column and retry. */
    OPTIMISTIC,

    /** One UPDATE that checks and subtracts together: "... WHERE available_stock >= quantity". */
    CONDITIONAL_UPDATE,

    /** A Redis lock per item (SET ... NX + expiry), so one buyer at a time, even across several app instances. */
    REDIS_LOCK
}
