package com.flashsale.payment;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

/**
 * The payment provider is down (it said so, or its circuit breaker is open), and NOTHING was charged.
 * The attempt was undone: the hold is back to HELD and no payment was recorded, so the client may send the
 * same request again (even with the same Idempotency-Key) after Retry-After seconds.
 */
public class PaymentProviderUnavailableException extends ApiException {

    private final int retryAfterSeconds;

    public PaymentProviderUnavailableException(PaymentProvider provider, int retryAfterSeconds) {
        super(HttpStatus.SERVICE_UNAVAILABLE, "PAYMENT_PROVIDER_UNAVAILABLE",
                "The " + provider + " payment provider is unavailable right now. Nothing was charged; please retry");
        this.retryAfterSeconds = retryAfterSeconds;
    }

    @Override
    public Integer retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
