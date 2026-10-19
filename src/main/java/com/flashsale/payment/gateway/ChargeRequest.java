package com.flashsale.payment.gateway;

import java.math.BigDecimal;

/**
 * @param idempotencyKey sent to the gateway so a retried call never charges twice
 * @param paymentToken   stands in for card details; real systems never see raw card numbers, only tokens
 */
public record ChargeRequest(String idempotencyKey, BigDecimal amount, String paymentToken) {
}
