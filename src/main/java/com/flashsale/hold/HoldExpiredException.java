package com.flashsale.hold;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

public class HoldExpiredException extends ApiException {

    public HoldExpiredException(Long holdId) {
        super(HttpStatus.CONFLICT, "HOLD_EXPIRED", "Hold " + holdId + " has expired; its stock went back on sale");
    }
}
