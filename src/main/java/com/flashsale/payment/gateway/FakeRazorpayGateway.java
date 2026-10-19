package com.flashsale.payment.gateway;

import org.springframework.stereotype.Component;

import com.flashsale.payment.PaymentProperties;
import com.flashsale.payment.PaymentProvider;

@Component
class FakeRazorpayGateway extends FakePaymentGateway {

    FakeRazorpayGateway(PaymentProperties properties) {
        super(properties.fakeLatency(), properties.gatewayTimeout());
    }

    @Override
    public PaymentProvider provider() {
        return PaymentProvider.RAZORPAY;
    }

    @Override
    protected String referencePrefix() {
        return "pay_";
    }
}
