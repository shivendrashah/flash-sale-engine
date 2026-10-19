package com.flashsale.payment.gateway;

import java.util.Optional;

import com.flashsale.payment.PaymentProvider;

/**
 * What every payment provider (Stripe, Razorpay, ...) must be able to do.
 */
public interface PaymentGateway {

    PaymentProvider provider();

    /**
     * Asks the provider to charge the card. Three ways it can end: an answer (approved or declined), a
     * GatewayUnavailableException (refused before charging: nothing charged) or a GatewayTimeoutException (no answer
     * in time: maybe charged).
     */
    ChargeResult charge(ChargeRequest request);

    /**
     * Asks the provider what happened to an earlier charge, by the idempotency key it was sent with. Empty means the
     * provider never received that charge. The payment recovery job uses this to finish payments whose answer was
     * lost.
     */
    Optional<ChargeResult> lookup(String idempotencyKey);
}
