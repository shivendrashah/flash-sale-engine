package com.flashsale.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs the outbox relay every 500 ms, in the background.
 *
 *       The other way to read an outbox is change data capture (Debezium reads PostgreSQL's write-ahead log and
 *       streams new rows to Kafka): no polling, but one more system to run (HLD.md 9.12).
 */
@Component
@ConditionalOnProperty(prefix = "flashsale.outbox", name = "relay-enabled", havingValue = "true",
        matchIfMissing = true)
class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxRelay relay;

    OutboxRelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${flashsale.outbox.relay-interval}")
    void publishPending() {
        int sent = relay.publishPending();
        if (sent > 0) {
            log.debug("Sent {} outbox event(s) to Kafka", sent);
        }
    }
}
