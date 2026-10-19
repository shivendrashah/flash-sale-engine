package com.flashsale.order;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderView(long orderId, long holdId, long paymentId, String userId, BigDecimal totalAmount,
                        OrderStatus status, Instant createdAt) {

    static OrderView of(Order order) {
        return new OrderView(order.getId(), order.getHoldId(), order.getPaymentId(), order.getUserId(),
                order.getTotalAmount(), order.getStatus(), order.getCreatedAt());
    }
}
