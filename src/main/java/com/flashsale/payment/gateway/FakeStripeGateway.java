package com.flashsale.payment.gateway;

import org.springframework.stereotype.Component;

import com.flashsale.payment.PaymentProperties;
import com.flashsale.payment.PaymentProvider;

@Component
class FakeStripeGateway extends FakePaymentGateway {

    FakeStripeGateway(PaymentProperties properties) {
        super(properties.fakeLatency(), properties.gatewayTimeout());
    }

    @Override
    public PaymentProvider provider() {
        return PaymentProvider.STRIPE;
    }

    @Override
    protected String referencePrefix() {
        return "pi_";
    }
}
