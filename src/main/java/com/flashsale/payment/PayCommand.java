package com.flashsale.payment;

import com.flashsale.common.RequestFingerprint;

/**
 * Everything PaymentService needs: who, which hold, the idempotency key, provider and card token.
 */
public record PayCommand(String userId, long holdId, String idempotencyKey, PaymentProvider provider,
                         String paymentToken) {

    /** Same hold + same provider + same card token = the same payment request. */
    public String fingerprint() {
        return RequestFingerprint.of(holdId, provider, paymentToken);
    }
}
