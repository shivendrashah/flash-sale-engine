package com.flashsale.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.common.KafkaTopics;
import com.flashsale.hold.HoldView;
import com.flashsale.outbox.OutboxEvent;
import com.flashsale.outbox.OutboxRelay;
import com.flashsale.outbox.OutboxRepository;
import com.flashsale.support.IntegrationTest;
import com.flashsale.support.KafkaTopicReader;

/**
 * The notification worker, end to end: pay -> outbox -> relay -> Kafka -> worker -> one message per order.
 * A second delivery of the same event is skipped; a customer whose mailbox refuses mail is retried, then parked
 * on the dead-letter topic without holding up anyone else; a message that can't be read goes straight there.
 */
class NotificationTest extends IntegrationTest {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @Autowired
    OutboxRelay relay;

    @Autowired
    OutboxRepository outboxEvents;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    ConsumerFactory<?, ?> consumers;

    @Autowired
    MeterRegistry meters;

    @Test
    void aPaidOrderReachesTheCustomer() {
        OutboxEvent event = buyAndRelay("alice");

        await().atMost(WAIT).until(() -> notificationsFor(event.getEventId()) == 1);
        assertThat(jdbc.queryForObject("SELECT message FROM notifications WHERE event_id = ?", String.class,
                event.getEventId())).isEqualTo("Your order #" + event.getAggregateId()
                + " is confirmed. Total: INR 59999.00");
    }

    @Test
    void theSameEventDeliveredTwiceNotifiesOnce() {
        OutboxEvent event = buyAndRelay("alice");
        await().atMost(WAIT).until(() -> notificationsFor(event.getEventId()) == 1);
        double duplicates = served("duplicate");

        // The relay's crash window: Kafka confirmed the message, but the app died before marking the row SENT. On
        // restart the row is still PENDING, so it is sent again.
        jdbc.update("UPDATE outbox_events SET status = 'PENDING' WHERE event_id = ?", event.getEventId());
        assertThat(relay.publishPending()).isEqualTo(1);

        await().atMost(WAIT).until(() -> served("duplicate") - duplicates == 1);
        assertThat(notificationsFor(event.getEventId())).isEqualTo(1);
    }

    @Test
    void aMailboxThatRefusesIsRetriedThenParkedOnTheDeadLetterTopic() {
        double deadLettered = served("dead_lettered");
        OutboxEvent bounced = buyAndRelay("bounce-bob");
        OutboxEvent fine = buyAndRelay("carol");

        await().atMost(WAIT).until(() -> served("dead_lettered") - deadLettered == 1);
        await().atMost(WAIT).until(() -> notificationsFor(fine.getEventId()) == 1);
        assertThat(notificationsFor(bounced.getEventId())).isZero();

        List<ConsumerRecord<String, String>> parked = KafkaTopicReader.read(consumers, KafkaTopics.ORDERS_DLT,
                KafkaTopicReader.eventIds(List.of(bounced.getEventId())), 1, WAIT);
        assertThat(parked).hasSize(1);
        assertThat(parked.getFirst().value()).as("the original message, unchanged").isEqualTo(bounced.getPayload());
        assertThat(KafkaTopicReader.header(parked.getFirst(), "kafka_dlt-exception-message"))
                .contains("does not accept mail");
    }

    @Test
    void aMessageThatCannotBeReadGoesStraightToTheDeadLetterTopic() {
        double unreadable = served("unreadable");
        UUID eventId = UUID.randomUUID();
        ProducerRecord<String, String> poison = new ProducerRecord<>(KafkaTopics.ORDERS, "mallory", "{not json");
        poison.headers().add("eventId", eventId.toString().getBytes(StandardCharsets.UTF_8));
        poison.headers().add("eventType", "ORDER_CONFIRMED".getBytes(StandardCharsets.UTF_8));
        kafka.send(poison).join();

        List<ConsumerRecord<String, String>> parked = KafkaTopicReader.read(consumers, KafkaTopics.ORDERS_DLT,
                KafkaTopicReader.eventIds(List.of(eventId)), 1, WAIT);
        assertThat(parked).hasSize(1);
        assertThat(KafkaTopicReader.header(parked.getFirst(), "kafka_dlt-exception-cause-fqcn"))
                .isEqualTo(UnreadableEventException.class.getName());
        // Read ONCE: no retries for a message that can't be read (with retries it would be 1 + 3 = 4)
        assertThat(served("unreadable") - unreadable).isEqualTo(1);
    }

    /** Buys one unit, pays, runs the relay; returns the order's outbox event (now SENT). */
    private OutboxEvent buyAndRelay(String userId) {
        HoldView hold = holdFor(userId, saleItemWithStock(1), 1);
        long orderId = pay(userId, hold.holdId(), "tok_success").orderId();
        assertThat(relay.publishPending()).isEqualTo(1);
        return outboxEvents.findByAggregateTypeAndAggregateIdOrderById("order", orderId).getFirst();
    }

    private int notificationsFor(UUID eventId) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM notifications WHERE event_id = ?", Integer.class, eventId);
    }

    /** Messages handled so far with this outcome (sent, duplicate, dead_lettered, unreadable). */
    private double served(String outcome) {
        return meters.counter("flashsale.notifications", "outcome", outcome).count();
    }
}
