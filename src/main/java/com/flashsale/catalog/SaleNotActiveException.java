package com.flashsale.catalog;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

public class SaleNotActiveException extends ApiException {

    public SaleNotActiveException(Long saleId) {
        super(HttpStatus.CONFLICT, "SALE_NOT_ACTIVE", "Sale " + saleId + " is not running right now");
    }
}
