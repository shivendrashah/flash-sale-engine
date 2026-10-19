package com.flashsale.outbox;

import java.util.UUID;

/**
 * An event that can be put in the outbox. The event record itself becomes the JSON payload; these methods say
 * where it goes and how it is identified.
 *
 *       implement this interface.
 */
public interface OutboxMessage {

    /** A random, never-repeating id. Consumers remember it to drop duplicate deliveries. */
    UUID eventId();

    /** What changed, e.g. "ORDER_CONFIRMED". Sent as the Kafka header "eventType". */
    String eventType();

    /** The kind of thing that changed, e.g. "order". */
    String aggregateType();

    /** Its id, e.g. the order id. */
    long aggregateId();

    /** The Kafka topic it goes to, e.g. "flashsale.orders" (KafkaTopics). */
    String topic();

    /**
     * The Kafka message key. Kafka sends all messages with the same key to the same partition, so they are read in
     * the order they were written.
     */
    String messageKey();
}
