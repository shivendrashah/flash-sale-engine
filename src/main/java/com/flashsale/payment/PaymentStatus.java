package com.flashsale.payment;

public enum PaymentStatus {
    /** We asked the gateway and are waiting for the answer. */
    PENDING,
    SUCCEEDED,
    FAILED
}
