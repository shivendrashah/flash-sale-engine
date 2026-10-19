package com.flashsale.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The read side of the outbox: sends PENDING events to Kafka and marks them SENT. If Kafka doesn't confirm an
 * event in time, the event stays PENDING and is tried again later, waiting longer after each failure.
 *   Kafka up:    every event is in Kafka within about half a second of its order.
 *   Kafka down:  orders keep being sold; events pile up in the table; once Kafka is back, they all go out.
 *
 *       commits, the rows are still PENDING and get sent again. So every consumer must drop duplicates by eventId
 *       (NotificationService does). That is the price of never losing an event (HLD.md 9.12).
 */
@Service
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository events;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final OutboxProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;

    public OutboxRelay(OutboxRepository events, KafkaTemplate<String, String> kafka, TransactionTemplate tx,
                       OutboxProperties properties, MeterRegistry meters, Clock clock) {
        this.events = events;
        this.kafka = kafka;
        this.tx = tx;
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
        Gauge.builder("flashsale.outbox.pending", events, repo -> repo.countByStatus(OutboxStatus.PENDING))
                .description("Outbox events not yet confirmed by Kafka")
                .register(meters);
    }

    /**
     * a failure: Kafka is probably down, and the rows wait for their next attempt time.
     *
     * @return how many events Kafka confirmed in this call
     */
    public int publishPending() {
        int total = 0;
        while (true) {
            Batch batch = tx.execute(status -> publishOneBatch());
            if (batch == null) {
                return total;
            }
            total += batch.sent();
            if (batch.anyFailed() || batch.claimed() < properties.batchSize()) {
                return total;
            }
        }
    }

    private Batch publishOneBatch() {
        Instant now = clock.instant();
        List<OutboxEvent> due = events.lockDue(now, properties.batchSize());
        if (due.isEmpty()) {
            return new Batch(0, 0, false);
        }

        List<CompletableFuture<?>> acks = new ArrayList<>(due.size());
        RuntimeException refused = null;
        for (OutboxEvent event : due) {
            if (refused != null) {
                acks.add(CompletableFuture.failedFuture(refused));   // don't wait max.block.ms once per event
                continue;
            }
            try {
                acks.add(kafka.send(toRecord(event)));
            } catch (RuntimeException e) {
                refused = e;
                acks.add(CompletableFuture.failedFuture(e));
            }
        }

        long deadline = System.nanoTime() + properties.sendTimeout().toNanos();
        int sent = 0;
        String firstError = null;
        for (int i = 0; i < due.size(); i++) {
            OutboxEvent event = due.get(i);
            String error = waitForAck(acks.get(i), deadline);
            if (error == null) {
                event.sent(clock.instant());
                sent++;
            } else {
                event.sendFailed(error, now, properties.retryBackoff(), properties.maxRetryBackoff());
                firstError = firstError == null ? error : firstError;
            }
        }
        meters.counter("flashsale.outbox.sent").increment(sent);
        int failed = due.size() - sent;
        if (failed > 0) {
            meters.counter("flashsale.outbox.send_failed").increment(failed);
            log.warn("Kafka did not confirm {} of {} outbox event(s); they stay PENDING and will be retried: {}",
                    failed, due.size(), firstError);
        }
        return new Batch(due.size(), sent, failed > 0);
    }

    /** @return null if Kafka confirmed the message before the deadline, else what went wrong */
    private static String waitForAck(CompletableFuture<?> ack, long deadline) {
        try {
            ack.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            return null;
        } catch (ExecutionException e) {
            Throwable cause = NestedExceptionUtils.getMostSpecificCause(e);
            return cause.getClass().getSimpleName() + ": " + cause.getMessage();
        } catch (TimeoutException e) {
            return "no answer from Kafka in time";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "interrupted";
        }
    }

    /** One message: topic, key (picks the partition), value (the JSON), and two headers. */
    private static ProducerRecord<String, String> toRecord(OutboxEvent event) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.getTopic(), event.getMessageKey(), event.getPayload());
        record.headers().add("eventId", event.getEventId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private record Batch(int claimed, int sent, boolean anyFailed) {
    }
}
