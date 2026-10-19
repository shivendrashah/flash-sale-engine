package com.flashsale.payment.gateway;

import com.flashsale.payment.PaymentProvider;

/**
 * The provider refused the call before charging anything: it answered "503, try later", or our own circuit
 * breaker didn't let the call out at all. The outcome is KNOWN: no money moved. PaymentService undoes the
 * attempt and answers 503 PAYMENT_PROVIDER_UNAVAILABLE.
 */
public class GatewayUnavailableException extends GatewayException {

    public GatewayUnavailableException(PaymentProvider provider, String message) {
        super(provider, message);
    }
}
