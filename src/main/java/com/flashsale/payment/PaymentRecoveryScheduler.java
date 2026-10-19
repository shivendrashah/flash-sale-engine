package com.flashsale.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs the payment recovery job every 30 seconds, in the background.
 */
@Component
@ConditionalOnProperty(prefix = "flashsale.payments", name = "recovery-job-enabled", havingValue = "true",
        matchIfMissing = true)
class PaymentRecoveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(PaymentRecoveryScheduler.class);

    private final PaymentRecoveryService recoveryService;

    PaymentRecoveryScheduler(PaymentRecoveryService recoveryService) {
        this.recoveryService = recoveryService;
    }

    @Scheduled(fixedDelayString = "${flashsale.payments.recovery-job-interval}")
    void recoverStuckPayments() {
        int recovered = recoveryService.recoverStuckPayments();
        if (recovered > 0) {
            log.info("Recovered {} stuck payment(s)", recovered);
        }
    }
}
