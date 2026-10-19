package com.flashsale.outbox;

public enum OutboxStatus {
    /** Committed with the business change; the relay has not had Kafka confirm it yet. */
    PENDING,
    /** Kafka confirmed it (acks=all: written to every in-sync copy of the partition). */
    SENT
}
