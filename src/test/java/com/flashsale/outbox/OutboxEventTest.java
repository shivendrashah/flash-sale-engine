package com.flashsale.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * An outbox event's retry timing while Kafka is down: 1 s, 2 s, 4 s ... doubling, never more than a minute
 * apart, and the error kept short enough for its column.
 */
class OutboxEventTest {

    private static final Instant NOW = Instant.parse("2026-10-10T10:00:00Z");

    @Test
    void eachFailureDoublesTheWaitUpToTheCap() {
        OutboxEvent event = OutboxEvent.pending(message(), "{}", NOW);
        List<Duration> waits = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            event.sendFailed("broker down", NOW, Duration.ofSeconds(1), Duration.ofMinutes(1));
            waits.add(Duration.between(NOW, event.getNextAttemptAt()));
        }

        assertThat(waits).extracting(Duration::toSeconds).containsExactly(1L, 2L, 4L, 8L, 16L, 32L, 60L, 60L, 60L);
        assertThat(event.getAttempts()).isEqualTo(9);
        assertThat(event.getStatus()).as("never given up on").isEqualTo(OutboxStatus.PENDING);
    }

    @Test
    void aVeryLongRunOfFailuresNeverOverflows() {
        OutboxEvent event = OutboxEvent.pending(message(), "{}", NOW);
        for (int i = 0; i < 100; i++) {    // 2^99 seconds would overflow a long
            event.sendFailed("broker down", NOW, Duration.ofSeconds(1), Duration.ofMinutes(1));
        }
        assertThat(event.getNextAttemptAt()).isEqualTo(NOW.plus(Duration.ofMinutes(1)));
    }

    @Test
    void aLongErrorIsCutToFitItsColumn() {
        OutboxEvent event = OutboxEvent.pending(message(), "{}", NOW);
        event.sendFailed("x".repeat(2000), NOW, Duration.ofSeconds(1), Duration.ofMinutes(1));
        assertThat(event.getLastError()).hasSize(500);

        event.sent(NOW);
        assertThat(event.getLastError()).isNull();
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.SENT);
    }

    private static OutboxMessage message() {
        return new OutboxMessage() {
            private final UUID id = UUID.randomUUID();

            public UUID eventId() {
                return id;
            }

            public String eventType() {
                return "TEST";
            }

            public String aggregateType() {
                return "test";
            }

            public long aggregateId() {
                return 1;
            }

            public String topic() {
                return "test";
            }

            public String messageKey() {
                return "k";
            }
        };
    }
}
