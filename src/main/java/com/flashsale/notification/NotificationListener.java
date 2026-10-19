package com.flashsale.notification;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import com.flashsale.common.KafkaTopics;
import com.flashsale.order.OrderConfirmedEvent;

/**
 * Reads every message on flashsale.orders. An ORDER_CONFIRMED event becomes a "your order is confirmed"
 * message to the customer; other event types are skipped (a newer app version may publish types this one
 * doesn't know yet).
 *
 *       database, so it could move to its own service without changing a line here (HLD.md 9.12).
 */
@Component
class NotificationListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    /** The consumer group. Every instance joins it, and Kafka shares the topic's partitions among them. */
    static final String GROUP = "notifications";

    private final NotificationService notifications;
    private final JsonMapper json;
    private final MeterRegistry meters;

    NotificationListener(NotificationService notifications, JsonMapper json, MeterRegistry meters) {
        this.notifications = notifications;
        this.json = json;
        this.meters = meters;
    }

    @KafkaListener(topics = KafkaTopics.ORDERS, groupId = GROUP)
    void onOrderEvent(ConsumerRecord<String, String> record) {
        String type = header(record, "eventType");
        if (!OrderConfirmedEvent.TYPE.equals(type)) {
            log.debug("Skipping event type {} at {}-{}@{}", type, record.topic(), record.partition(), record.offset());
            return;
        }
        OrderConfirmedEvent event;
        try {
            event = json.readValue(record.value(), OrderConfirmedEvent.class);
        } catch (JacksonException e) {
            meters.counter("flashsale.notifications", "outcome", "unreadable").increment();
            throw new UnreadableEventException("Unreadable " + type + " at " + record.topic() + "-"
                    + record.partition() + "@" + record.offset(), e);
        }
        if (event.eventId() == null || event.userId() == null || event.totalAmount() == null) {
            meters.counter("flashsale.notifications", "outcome", "unreadable").increment();
            throw new UnreadableEventException("Incomplete " + type + " at " + record.topic() + "-"
                    + record.partition() + "@" + record.offset(), null);
        }
        notifications.orderConfirmed(event);
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
