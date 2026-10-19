package com.flashsale.payment.gateway;

import java.time.Duration;

import com.flashsale.payment.PaymentProvider;

/**
 * No answer within flashsale.payments.gateway-timeout. The outcome is UNKNOWN: the provider may have charged
 * the card and its answer got lost, or the request may never have arrived. PaymentService must not call this
 * a failure: it leaves the payment PENDING, and the recovery job asks the provider later.
 */
public class GatewayTimeoutException extends GatewayException {

    public GatewayTimeoutException(PaymentProvider provider, Duration timeout) {
        super(provider, "no answer within " + timeout.toMillis() + " ms");
    }
}
