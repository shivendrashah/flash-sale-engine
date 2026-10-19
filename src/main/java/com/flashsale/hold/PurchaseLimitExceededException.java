package com.flashsale.hold;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

/**
 * The buyer would go over the sale's per-user limit. HTTP 409 with code PURCHASE_LIMIT_EXCEEDED: the request
 * conflicts with what the buyer already holds or bought. Nothing is taken.
 */
public class PurchaseLimitExceededException extends ApiException {

    /**
     * @param held  units the buyer already holds or bought in this sale
     * @param asked units this request needs (at least)
     */
    public PurchaseLimitExceededException(long saleId, int limit, int held, int asked) {
        super(HttpStatus.CONFLICT, "PURCHASE_LIMIT_EXCEEDED", "Sale " + saleId + " allows at most " + limit
                + " unit(s) per buyer. You already hold or bought " + held + ", and this request needs at least "
                + asked + " more");
    }
}
