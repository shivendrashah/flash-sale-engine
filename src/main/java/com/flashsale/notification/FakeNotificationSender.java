package com.flashsale.notification;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * "Sends" a notification by writing it to the log. Like the fake payment tokens, one magic value makes it
 * fail, so retries and the dead-letter topic can be tried by hand and tested:
 *   a user id starting with "bounce"   ->  NotificationFailedException, every time
 */
@Component
class FakeNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(FakeNotificationSender.class);

    @Override
    public void send(UUID eventId, String userId, String message) {
        if (userId.startsWith("bounce")) {
            throw new NotificationFailedException("Mailbox of " + userId + " does not accept mail");
        }
        log.info("Notification to {}: {} (event {})", userId, message, eventId);
    }
}
