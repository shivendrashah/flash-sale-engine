package com.flashsale.notification;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.order.OrderConfirmedEvent;

/**
 * Tells the customer their order is confirmed, ONCE, even when Kafka delivers the same event twice:
 *   event seen before   ->  skip it ("duplicate")
 *   new event           ->  send the message, then record the event id in the notifications table
 *
 *       least once", so duplicates WILL arrive: after a relay crash, after a consumer crash before it commits its
 *       offset, after a retry. Remembering each event id turns that into one message per order.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final JdbcTemplate jdbc;
    private final NotificationSender sender;
    private final MeterRegistry meters;
    private final Clock clock;

    public NotificationService(JdbcTemplate jdbc, NotificationSender sender, MeterRegistry meters, Clock clock) {
        this.jdbc = jdbc;
        this.sender = sender;
        this.meters = meters;
        this.clock = clock;
    }

    /**
     * @throws NotificationFailedException if the provider failed; nothing is recorded, so the retry sends it again
     */
    public void orderConfirmed(OrderConfirmedEvent event) {
        Integer seen = jdbc.queryForObject("SELECT COUNT(*) FROM notifications WHERE event_id = ?", Integer.class,
                event.eventId());
        if (seen != null && seen > 0) {
            log.info("Event {} for order {} already handled: duplicate delivery skipped", event.eventId(),
                    event.orderId());
            count("duplicate");
            return;
        }

        String message = "Your order #" + event.orderId() + " is confirmed. Total: INR "
                + event.totalAmount().toPlainString();
        sender.send(event.eventId(), event.userId(), message);

        jdbc.update("""
                INSERT INTO notifications (event_id, user_id, order_id, message, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """, event.eventId(), event.userId(), event.orderId(), message,
                java.sql.Timestamp.from(clock.instant()));
        count("sent");
    }

    private void count(String outcome) {
        meters.counter("flashsale.notifications", "outcome", outcome).increment();
    }
}
