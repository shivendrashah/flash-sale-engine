package com.flashsale.catalog;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

public class SoldOutException extends ApiException {

    public SoldOutException(Long saleItemId, int requested) {
        this("Sale item " + saleItemId + " does not have " + requested + " unit(s) left");
    }

    private SoldOutException(String message) {
        super(HttpStatus.CONFLICT, "SOLD_OUT", message);
    }

    /** A PARTIAL cart where not one item could be held. A named static factory reads better than a flag. */
    public static SoldOutException noneLeft(int requestedItems) {
        return new SoldOutException("None of the " + requestedItems + " requested item(s) has enough stock left");
    }
}
