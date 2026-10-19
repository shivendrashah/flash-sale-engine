package com.flashsale.outbox;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The settings under "flashsale.outbox": how often the relay looks for events to send, how many per
 * transaction, how long it waits for Kafka, and how it backs off while Kafka is down.
 */
@ConfigurationProperties("flashsale.outbox")
@Validated
public record OutboxProperties(
        boolean relayEnabled,              // flashsale.outbox.relay-enabled: false = no OutboxRelayScheduler (tests)
        @NotNull Duration relayInterval,   // ...relay-interval: pause between two runs of the relay
        @Positive int batchSize,           // ...batch-size: events claimed per transaction
        // ...send-timeout: the longest the relay waits for Kafka to confirm one batch
        @NotNull @DurationMin(millis = 1) Duration sendTimeout,
        @NotNull @DurationMin(millis = 1) Duration retryBackoff,      // ...retry-backoff: the wait after a 1st failure
        @NotNull @DurationMin(millis = 1) Duration maxRetryBackoff) { // ...max-retry-backoff: the longest wait
}
