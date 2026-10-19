package com.flashsale.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;

import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.hold.HoldStatus;
import com.flashsale.hold.HoldView;
import com.flashsale.payment.PaymentStatus;
import com.flashsale.payment.PaymentView;
import com.flashsale.support.IntegrationTest;

/**
 * Phase 3's "done when": a notification survives a Kafka outage.
 *   Kafka down:   a buyer still pays and gets their order at once; the event waits in the outbox (PENDING),
 *                 and the relay backs off instead of hammering the broker.
 *   Kafka back:   the next relay run sends it, and the customer is notified.
 */
@TestPropertySource(properties = {
        "spring.kafka.producer.properties.max.block.ms=1000",
        "spring.kafka.producer.properties.request.timeout.ms=1000",
        "spring.kafka.producer.properties.delivery.timeout.ms=1500",
        "flashsale.outbox.send-timeout=2s"
})
class KafkaOutageTest extends IntegrationTest {

    @Autowired
    KafkaContainer kafkaContainer;

    @Autowired
    OutboxRelay relay;

    @Autowired
    OutboxRepository outboxEvents;

    @Autowired
    MeterRegistry meters;

    private boolean paused;

    @AfterEach
    void kafkaIsBackForTheNextTest() {
        if (paused) {
            unpauseKafka();
        }
    }

    @Test
    void anOrderPaidWhileKafkaIsDownIsAnnouncedOnceKafkaIsBack() {
        HoldView hold = holdFor("alice", saleItemWithStock(1), 1);
        pauseKafka();

        // 1. Paying doesn't touch Kafka at all: the event is just one more row in the payment's transaction.
        long started = System.nanoTime();
        PaymentView payment = pay("alice", hold.holdId(), "tok_success");
        assertThat(Duration.ofNanos(System.nanoTime() - started)).as("no Kafka timeout in the buyer's request")
                .isLessThan(Duration.ofMillis(500));
        assertThat(payment.paymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(payment.holdStatus()).isEqualTo(HoldStatus.CONFIRMED);

        // 2. The relay can't get it to Kafka: the event stays PENDING, with a note of what went wrong and when to try
        //    next.
        double failedBefore = meters.counter("flashsale.outbox.send_failed").count();
        assertThat(relay.publishPending()).isZero();
        OutboxEvent waiting = onlyEvent();
        assertThat(waiting.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(waiting.getAttempts()).isEqualTo(1);
        assertThat(waiting.getLastError()).isNotBlank();
        assertThat(waiting.getNextAttemptAt()).isEqualTo(clock.instant().plus(Duration.ofSeconds(1)));
        assertThat(meters.counter("flashsale.outbox.send_failed").count() - failedBefore).isEqualTo(1);

        // 3. Back-off: running the relay again right away doesn't even try (the row isn't due), so it costs nothing.
        long again = System.nanoTime();
        assertThat(relay.publishPending()).isZero();
        assertThat(Duration.ofNanos(System.nanoTime() - again)).isLessThan(Duration.ofMillis(500));
        assertThat(onlyEvent().getAttempts()).as("not tried again yet").isEqualTo(1);

        // 4. Kafka comes back. Once the wait is over, the next run sends the event, and the customer hears about it.
        //    (A broker that just woke up may need a moment: each loop jumps past the next wait and tries again.)
        unpauseKafka();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            clock.advance(Duration.ofMinutes(1));
            return relay.publishPending() == 1;
        });
        OutboxEvent sent = onlyEvent();
        assertThat(sent.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(sent.getAttempts()).isGreaterThanOrEqualTo(2);
        await().atMost(Duration.ofSeconds(30)).until(() -> jdbc.queryForObject(
                "SELECT COUNT(*) FROM notifications WHERE event_id = ?", Integer.class, sent.getEventId()) == 1);
    }

    @Test
    void whileKafkaIsDownEachFailureWaitsLongerBeforeTheNextTry() {
        HoldView hold = holdFor("alice", saleItemWithStock(1), 1);
        pay("alice", hold.holdId(), "tok_success");
        pauseKafka();

        Instant start = clock.instant();
        for (long expectedWait : new long[] {1, 2, 4}) {
            relay.publishPending();
            OutboxEvent event = onlyEvent();
            assertThat(Duration.between(clock.instant(), event.getNextAttemptAt()).toSeconds())
                    .isEqualTo(expectedWait);
            clock.advance(Duration.ofSeconds(expectedWait));   // jump to the next try
        }
        assertThat(onlyEvent().getAttempts()).isEqualTo(3);
        assertThat(Duration.between(start, clock.instant()).toSeconds()).isEqualTo(7);   // 1 + 2 + 4
    }

    private OutboxEvent onlyEvent() {
        assertThat(outboxEvents.count()).isEqualTo(1);
        return outboxEvents.findAll().getFirst();
    }

    private void pauseKafka() {
        DockerClientFactory.instance().client().pauseContainerCmd(kafkaContainer.getContainerId()).exec();
        paused = true;
    }

    private void unpauseKafka() {
        DockerClientFactory.instance().client().unpauseContainerCmd(kafkaContainer.getContainerId()).exec();
        paused = false;
    }
}
