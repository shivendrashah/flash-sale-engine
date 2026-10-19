package com.flashsale.payment.gateway;

import com.flashsale.payment.PaymentProvider;

/**
 * A call to a payment provider produced no answer. The two subclasses say what we know afterwards:
 *   GatewayUnavailableException  the provider refused before charging: nothing was charged (known)
 *   GatewayTimeoutException      no answer in time: the card MAY have been charged (unknown)
 * PaymentService handles them very differently, and the circuit breaker counts both as failures.
 * A declined card is NOT one of these: that is a normal answer (ChargeResult.declined).
 */
public abstract class GatewayException extends RuntimeException {

    private final PaymentProvider provider;

    protected GatewayException(PaymentProvider provider, String message) {
        super(provider + ": " + message);
        this.provider = provider;
    }

    public PaymentProvider provider() {
        return provider;
    }
}
