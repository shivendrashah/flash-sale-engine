package com.flashsale.hold;

import java.util.Collection;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

public class MixedSalesException extends ApiException {

    public MixedSalesException(Collection<Long> saleIds) {
        super(HttpStatus.UNPROCESSABLE_CONTENT, "MIXED_SALES",
                "All items in one hold must come from the same sale, but these items are in sales " + saleIds);
    }
}
