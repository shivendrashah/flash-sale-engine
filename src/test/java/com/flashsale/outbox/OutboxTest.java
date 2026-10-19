package com.flashsale.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import com.flashsale.common.KafkaTopics;
import com.flashsale.hold.HoldView;
import com.flashsale.order.OrderConfirmedEvent;
import com.flashsale.payment.PaymentRecoveryService;
import com.flashsale.payment.PaymentStatus;
import com.flashsale.support.Concurrently;
import com.flashsale.support.IntegrationTest;
import com.flashsale.support.KafkaTopicReader;

/**
 * The write side and the relay of the transactional outbox:
 *   a paid order writes ONE event row, in the same transaction (a decline or a rollback writes none);
 *   the relay puts it on Kafka with its key and headers and marks it SENT;
 *   four relays running at once send each event exactly once (FOR UPDATE SKIP LOCKED).
 */
class OutboxTest extends IntegrationTest {

    @Autowired
    OutboxRelay relay;

    @Autowired
    OutboxRepository outboxEvents;

    @Autowired
    Outbox outbox;

    @Autowired
    PaymentRecoveryService recovery;

    @Autowired
    TransactionTemplate tx;

    @Autowired
    ConsumerFactory<?, ?> consumers;

    @Autowired
    JsonMapper json;

    @Autowired
    MeterRegistry meters;

    @Test
    void aPaidOrderWritesOneEventInTheSameTransaction() throws Exception {
        long orderId = buyOne("alice");

        List<OutboxEvent> events = outboxEvents.findByAggregateTypeAndAggregateIdOrderById("order", orderId);
        assertThat(events).hasSize(1);
        OutboxEvent event = events.getFirst();
        assertThat(event.getStatus()).as("not in Kafka yet: the relay hasn't run").isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getEventType()).isEqualTo("ORDER_CONFIRMED");
        assertThat(event.getTopic()).isEqualTo(KafkaTopics.ORDERS);
        assertThat(event.getMessageKey()).isEqualTo("alice");

        JsonNode payload = json.readTree(event.getPayload());
        assertThat(payload.get("eventId").asString()).isEqualTo(event.getEventId().toString());
        assertThat(payload.get("orderId").asLong()).isEqualTo(orderId);
        assertThat(payload.get("userId").asString()).isEqualTo("alice");
        assertThat(payload.get("totalAmount").decimalValue()).isEqualByComparingTo("59999.00");
        assertThat(payload.has("topic")).as("routing details are not part of the payload").isFalse();
    }

    @Test
    void aDeclinedPaymentWritesNoEvent() {
        HoldView hold = holdFor("alice", saleItemWithStock(1), 1);
        assertThat(pay("alice", hold.holdId(), "tok_declined").paymentStatus()).isEqualTo(PaymentStatus.FAILED);

        assertThat(count("outbox_events")).isZero();
    }

    @Test
    void anOrderFinishedByTheRecoveryJobWritesItsEventToo() {
        HoldView hold = holdFor("alice", saleItemWithStock(1), 1);
        pay("alice", hold.holdId(), "tok_timeout");                 // charged, but the answer was lost: 202
        assertThat(count("outbox_events")).isZero();

        clock.advance(Duration.ofMinutes(2));
        assertThat(recovery.recoverStuckPayments()).isEqualTo(1);   // the same step B5, so the same event

        assertThat(count("outbox_events")).isEqualTo(1);
    }

    @Test
    void anEventIsRolledBackWithItsTransaction() {
        tx.executeWithoutResult(status -> {
            outbox.add(event("alice"));
            status.setRollbackOnly();            // as if the order's INSERT had failed after this line
        });
        assertThat(count("outbox_events")).isZero();
    }

    @Test
    void writingAnEventOutsideATransactionIsRefused() {
        assertThatThrownBy(() -> outbox.add(event("alice"))).isInstanceOf(IllegalTransactionStateException.class);
        assertThat(count("outbox_events")).isZero();
    }

    @Test
    void theRelaySendsTheEventToKafkaAndMarksItSent() {
        long orderId = buyOne("alice");
        OutboxEvent pending = outboxEvents.findByAggregateTypeAndAggregateIdOrderById("order", orderId).getFirst();

        assertThat(relay.publishPending()).isEqualTo(1);

        OutboxEvent sent = outboxEvents.findByEventId(pending.getEventId()).orElseThrow();
        assertThat(sent.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(sent.getAttempts()).isEqualTo(1);
        assertThat(sent.getSentAt()).isEqualTo(clock.instant());
        assertThat(relay.publishPending()).as("nothing left to send").isZero();

        List<ConsumerRecord<String, String>> onKafka = KafkaTopicReader.read(consumers, KafkaTopics.ORDERS,
                KafkaTopicReader.eventIds(List.of(pending.getEventId())), 1, Duration.ofSeconds(10));
        assertThat(onKafka).hasSize(1);
        ConsumerRecord<String, String> message = onKafka.getFirst();
        assertThat(message.key()).isEqualTo("alice");
        assertThat(message.value()).isEqualTo(pending.getPayload());
        assertThat(KafkaTopicReader.header(message, "eventType")).isEqualTo("ORDER_CONFIRMED");
    }

    @Test
    void fourRelaysAtOnceSendEachEventExactlyOnce() {
        List<UUID> eventIds = new ArrayList<>();
        tx.executeWithoutResult(status -> {
            for (int i = 0; i < 250; i++) {          // 3 batches of 100, so the relays really overlap
                OrderConfirmedEvent event = event("buyer-" + i);
                outbox.add(event);
                eventIds.add(event.eventId());
            }
        });
        double sentBefore = meters.counter("flashsale.outbox.sent").count();

        List<Concurrently.Outcome<Integer>> relays = Concurrently.run(4, i -> relay.publishPending());

        assertThat(Concurrently.failures(relays)).isEmpty();
        assertThat(relays.stream().mapToInt(outcome -> outcome.value()).sum()).as("sent, over all relays")
                .isEqualTo(250);
        assertThat(meters.counter("flashsale.outbox.sent").count() - sentBefore).isEqualTo(250);
        assertThat(outboxEvents.countByStatus(OutboxStatus.PENDING)).isZero();
        // ...and Kafka got 250 messages, not more: no event went out twice
        List<ConsumerRecord<String, String>> onKafka = KafkaTopicReader.read(consumers, KafkaTopics.ORDERS,
                KafkaTopicReader.eventIds(eventIds), 250, Duration.ofSeconds(20));
        assertThat(onKafka).hasSize(250);
    }

    /** Holds and pays for one unit; returns the order id. */
    private long buyOne(String userId) {
        HoldView hold = holdFor(userId, saleItemWithStock(1), 1);
        return pay(userId, hold.holdId(), "tok_success").orderId();
    }

    private OrderConfirmedEvent event(String userId) {
        return new OrderConfirmedEvent(UUID.randomUUID(), 1, 1, userId, new java.math.BigDecimal("100.00"),
                clock.instant());
    }
}
