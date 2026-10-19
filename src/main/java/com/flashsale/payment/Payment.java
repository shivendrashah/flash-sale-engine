package com.flashsale.payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * One attempt to pay for a hold. A hold can have several attempts (a declined card, then a good one).
 */
@Entity
@Table(name = "payments")
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "hold_id", nullable = false)
    private Long holdId;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentProvider provider;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    /** The gateway's id for this charge, e.g. "pi_..." from Stripe. Needed for refunds and support tickets. */
    @Column(name = "provider_ref", length = 100)
    private String providerRef;

    @Column(name = "failure_reason", length = 200)
    private String failureReason;

    @Column(name = "idempotency_key", nullable = false, length = 100)
    private String idempotencyKey;

    @Column(name = "request_hash", nullable = false, length = 64)
    private String requestHash;

    /**
     * The idempotency key we send to the gateway: unique per attempt, the same on every retry of that attempt.
     * Random rather than "payment-" + id, because ids can repeat (database restore, two environments sharing
     * one gateway account) and the gateway would then replay someone else's old answer.
     */
    @Column(name = "gateway_key", nullable = false, length = 64)
    private String gatewayKey;

    @Version
    @Column(nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Payment() {
    }

    /** A new attempt starts as PENDING, before we call the gateway. */
    public static Payment start(long holdId, String userId, PaymentProvider provider, BigDecimal amount,
                                String idempotencyKey, String requestHash, Instant now) {
        Payment payment = new Payment();
        payment.holdId = holdId;
        payment.userId = userId;
        payment.provider = provider;
        payment.amount = amount;
        payment.status = PaymentStatus.PENDING;
        payment.idempotencyKey = idempotencyKey;
        payment.requestHash = requestHash;
        payment.gatewayKey = UUID.randomUUID().toString();
        payment.createdAt = now;
        payment.updatedAt = now;
        return payment;
    }

    /** The gateway approved. (In memory; Hibernate sends the UPDATE at commit.) */
    public void succeeded(String providerRef, Instant now) {
        requirePending();
        this.status = PaymentStatus.SUCCEEDED;
        this.providerRef = providerRef;
        this.updatedAt = now;
    }

    /** The gateway declined. */
    public void failed(String reason, Instant now) {
        requirePending();
        this.status = PaymentStatus.FAILED;
        this.failureReason = reason;
        this.updatedAt = now;
    }

    private void requirePending() {
        if (status != PaymentStatus.PENDING) {
            throw new IllegalStateException("Payment " + id + " is already " + status);
        }
    }

    public String getGatewayKey() {
        return gatewayKey;
    }

    public Long getId() {
        return id;
    }

    public Long getHoldId() {
        return holdId;
    }

    public PaymentProvider getProvider() {
        return provider;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getProviderRef() {
        return providerRef;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public String getRequestHash() {
        return requestHash;
    }
}
