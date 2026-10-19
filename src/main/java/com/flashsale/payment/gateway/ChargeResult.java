package com.flashsale.payment.gateway;

public record ChargeResult(boolean succeeded, String providerReference, String failureReason) {

    public static ChargeResult approved(String providerReference) {
        return new ChargeResult(true, providerReference, null);
    }

    public static ChargeResult declined(String reason) {
        return new ChargeResult(false, null, reason);
    }
}
