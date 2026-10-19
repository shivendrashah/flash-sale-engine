package com.flashsale.inventory;

/**
 * What the buyer wants when SOME items of a cart are sold out. Sent in the hold request's JSON body:
 *   {"items": [...], "mode": "PARTIAL"}      (leave "mode" out for ALL_OR_NOTHING)
 */
public enum ReservationMode {

    /** Every item, or nothing. One short item rejects the whole cart (409 SOLD_OUT) and nothing is held. */
    ALL_OR_NOTHING,

    /** Hold what is available, skip what is not, and list the skipped items in the response ("unavailable"). */
    PARTIAL
}
