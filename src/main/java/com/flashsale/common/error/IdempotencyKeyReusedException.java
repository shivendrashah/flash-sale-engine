package com.flashsale.common.error;

import org.springframework.http.HttpStatus;

public class IdempotencyKeyReusedException extends ApiException {

    public IdempotencyKeyReusedException(String idempotencyKey) {
        super(HttpStatus.UNPROCESSABLE_CONTENT, "IDEMPOTENCY_KEY_REUSED",
                "Idempotency-Key '" + idempotencyKey + "' was already used for a different request");
    }
}
