package com.flashsale.order;

import java.math.BigDecimal;
import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * The permanent record of a purchase. A hold is temporary; an order is what the customer bought.
 *
 *       the hold or payment packages.
 */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "hold_id", nullable = false)
    private Long holdId;

    @Column(name = "payment_id", nullable = false)
    private Long paymentId;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Order() {
    }

    public static Order place(long holdId, long paymentId, String userId, BigDecimal totalAmount, Instant now) {
        Order order = new Order();
        order.holdId = holdId;
        order.paymentId = paymentId;
        order.userId = userId;
        order.totalAmount = totalAmount;
        order.status = OrderStatus.CONFIRMED;
        order.createdAt = now;
        return order;
    }

    public Long getId() {
        return id;
    }

    public Long getHoldId() {
        return holdId;
    }

    public Long getPaymentId() {
        return paymentId;
    }

    public String getUserId() {
        return userId;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
