package com.flashsale.hold;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

public class PaymentInProgressException extends ApiException {

    public PaymentInProgressException(Long holdId) {
        super(HttpStatus.CONFLICT, "PAYMENT_IN_PROGRESS", "A payment for hold " + holdId + " is already running");
    }
}
