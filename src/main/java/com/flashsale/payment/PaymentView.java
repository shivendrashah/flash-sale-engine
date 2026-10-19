package com.flashsale.payment;

import java.math.BigDecimal;

import com.flashsale.hold.HoldStatus;

/**
 * What the API returns after a payment attempt (JSON). orderId is set only when the payment succeeded.
 */
public record PaymentView(long paymentId, long holdId, PaymentStatus paymentStatus, HoldStatus holdStatus,
                          PaymentProvider provider, BigDecimal amount, String providerReference,
                          String failureReason, Long orderId) {
}
