package com.flashsale.outbox;

import java.time.Duration;
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

/**
 * One event in the outbox table: saved in the same transaction as the business change, sent to Kafka later.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    /** The longest a single error message is kept (the column is VARCHAR(500)). */
    private static final int MAX_ERROR_LENGTH = 500;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(name = "aggregate_type", nullable = false, length = 50, updatable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, updatable = false)
    private long aggregateId;

    @Column(name = "event_type", nullable = false, length = 50, updatable = false)
    private String eventType;

    @Column(nullable = false, length = 100, updatable = false)
    private String topic;

    @Column(name = "message_key", nullable = false, length = 100, updatable = false)
    private String messageKey;

    /** The event as JSON: exactly the bytes Kafka will carry. */
    @Column(nullable = false, updatable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OutboxStatus status;

    @Column(nullable = false)
    private int attempts;

    /** The relay leaves the row alone until this time (now for a new row; later after each failed send). */
    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "last_error", length = MAX_ERROR_LENGTH)
    private String lastError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "sent_at")
    private Instant sentAt;

    protected OutboxEvent() {
    }

    /** A new event, due at once. */
    static OutboxEvent pending(OutboxMessage message, String payload, Instant now) {
        OutboxEvent event = new OutboxEvent();
        event.eventId = message.eventId();
        event.aggregateType = message.aggregateType();
        event.aggregateId = message.aggregateId();
        event.eventType = message.eventType();
        event.topic = message.topic();
        event.messageKey = message.messageKey();
        event.payload = payload;
        event.status = OutboxStatus.PENDING;
        event.nextAttemptAt = now;
        event.createdAt = now;
        return event;
    }

    /** Kafka confirmed it. (In memory; Hibernate sends the UPDATE at commit.) */
    void sent(Instant now) {
        this.status = OutboxStatus.SENT;
        this.attempts++;
        this.sentAt = now;
        this.lastError = null;
    }

    /**
     * waiting longer than {@code maxWait}: 1 s, 2 s, 4 s ... 60 s, 60 s ...
     */
    void sendFailed(String error, Instant now, Duration firstWait, Duration maxWait) {
        this.attempts++;
        Duration wait = firstWait.multipliedBy(1L << Math.min(attempts - 1, 20));
        this.nextAttemptAt = now.plus(wait.compareTo(maxWait) > 0 ? maxWait : wait);
        this.lastError = error == null || error.length() <= MAX_ERROR_LENGTH
                ? error
                : error.substring(0, MAX_ERROR_LENGTH);
    }

    public Long getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public long getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLastError() {
        return lastError;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getSentAt() {
        return sentAt;
    }
}
