package com.flashsale.outbox;

import java.time.Clock;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.json.JsonMapper;

/**
 * Puts an event in the outbox table, inside the caller's transaction. It does NOT talk to Kafka: OutboxRelay
 * sends the row later. That is the whole trick:
 *   the order and its event commit together  ->  no order without its event, no event for an order that
 *                                                rolled back, and Kafka being down never blocks a purchase.
 */
@Component
public class Outbox {

    private final OutboxRepository events;
    private final JsonMapper json;
    private final Clock clock;

    public Outbox(OutboxRepository events, JsonMapper json, Clock clock) {
        this.events = events;
        this.json = json;
        this.clock = clock;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void add(OutboxMessage message) {
        String payload = json.writeValueAsString(message);
        events.save(OutboxEvent.pending(message, payload, clock.instant()));
    }
}
