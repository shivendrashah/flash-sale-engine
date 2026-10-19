package com.flashsale.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.flashsale.common.KafkaTopics;
import com.flashsale.outbox.OutboxMessage;

/**
 * "Order 42 was confirmed for user alice, total 59999.00." Written to the outbox with the order, sent to the
 * Kafka topic flashsale.orders, read by the notification worker. As JSON:
 *   {"eventId":"6f1c...","orderId":42,"holdId":7,"userId":"alice","totalAmount":59999.00,
 *    "confirmedAt":"2026-10-10T10:00:00Z"}
 *
 *       The order package publishes it without knowing who listens. Adding an SMS or an analytics consumer later
 *       needs no change here.
 *       app's database ("event-carried state transfer").
 */
public record OrderConfirmedEvent(UUID eventId, long orderId, long holdId, String userId, BigDecimal totalAmount,
                                  Instant confirmedAt) implements OutboxMessage {

    public static final String TYPE = "ORDER_CONFIRMED";

    public static OrderConfirmedEvent of(Order order) {
        return new OrderConfirmedEvent(UUID.randomUUID(), order.getId(), order.getHoldId(), order.getUserId(),
                order.getTotalAmount(), order.getCreatedAt());
    }

    @Override
    public String eventType() {
        return TYPE;
    }

    @Override
    public String aggregateType() {
        return "order";
    }

    @Override
    public long aggregateId() {
        return orderId;
    }

    @Override
    public String topic() {
        return KafkaTopics.ORDERS;
    }

    /** Keyed by user: one user's messages share a partition, so they arrive in order. */
    @Override
    public String messageKey() {
        return userId;
    }
}
