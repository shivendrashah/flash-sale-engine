package com.flashsale.notification;

import java.util.UUID;

/**
 * Sends one message to a customer (an email, an SMS, a push notification).
 */
public interface NotificationSender {

    /**
     * Sends the message.
     *
     * @param eventId passed on as the provider's idempotency key: real email APIs accept one, so a retry after a
     *                crash (sent, but not yet recorded) doesn't email the customer twice
     * @throws NotificationFailedException if the provider refused or could not be reached (worth retrying)
     */
    void send(UUID eventId, String userId, String message);
}
