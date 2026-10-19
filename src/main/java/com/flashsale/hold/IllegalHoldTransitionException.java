package com.flashsale.hold;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

public class IllegalHoldTransitionException extends ApiException {

    public IllegalHoldTransitionException(Long holdId, HoldStatus from, HoldStatus to) {
        super(HttpStatus.CONFLICT, "ILLEGAL_STATE_TRANSITION",
                "Hold " + holdId + " is " + from + " and cannot become " + to);
    }
}
