package com.flashsale.hold;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs the expiry job every few seconds, in the background.
 *
 *               same time. That's fine: the query uses FOR UPDATE SKIP LOCKED, so each copy takes different holds.
 */
@Component
@ConditionalOnProperty(prefix = "flashsale.holds", name = "expiry-job-enabled", havingValue = "true",
        matchIfMissing = true)
class HoldExpiryScheduler {

    private static final Logger log = LoggerFactory.getLogger(HoldExpiryScheduler.class);

    private final HoldExpiryService expiryService;

    HoldExpiryScheduler(HoldExpiryService expiryService) {
        this.expiryService = expiryService;
    }

    @Scheduled(fixedDelayString = "${flashsale.holds.expiry-job-interval}")
    void expireDueHolds() {
        int expired = expiryService.expireDueHolds();
        if (expired > 0) {
            log.info("Expired {} hold(s) and put their stock back on sale", expired);
        }
    }
}
