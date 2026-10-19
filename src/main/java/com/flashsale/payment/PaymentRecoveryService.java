package com.flashsale.payment;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.payment.gateway.ChargeResult;
import com.flashsale.payment.gateway.GatewayException;
import com.flashsale.payment.gateway.PaymentGatewayFactory;

/**
 * Finishes payments that got stuck in PENDING: the provider's answer never reached us (a timeout), or the app
 * crashed between the gateway call (B4) and saving its outcome (B5). For each one it asks the provider "what
 * happened to this charge?" and then does step B5 exactly as a normal payment would:
 *   the provider charged it     -> payment SUCCEEDED, hold CONFIRMED, order created
 *   the provider declined it    -> payment FAILED, hold back to HELD
 *   the provider never saw it   -> payment FAILED ("never_reached_provider"), hold back to HELD
 *   the provider can't answer   -> left PENDING; the next run asks again
 *
 *               step B5 locks the hold row and does nothing if the payment was already finished.
 */
@Service
public class PaymentRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(PaymentRecoveryService.class);

    /** The failure reason saved when the provider has no record of the charge. */
    static final String NEVER_REACHED_PROVIDER = "never_reached_provider";

    private final PaymentRepository payments;
    private final PaymentGatewayFactory gateways;
    private final PaymentService paymentService;
    private final TransactionTemplate tx;
    private final PaymentProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;

    public PaymentRecoveryService(PaymentRepository payments, PaymentGatewayFactory gateways,
                                  PaymentService paymentService, TransactionTemplate tx, PaymentProperties properties,
                                  MeterRegistry meters, Clock clock) {
        this.payments = payments;
        this.gateways = gateways;
        this.paymentService = paymentService;
        this.tx = tx;
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
    }

    /** @return how many stuck payments this run finished (the rest are left for the next run) */
    public int recoverStuckPayments() {
        Instant cutoff = clock.instant().minus(properties.recoverAfter());
        List<Payment> stuck = payments.findStuck(cutoff, Limit.of(properties.recoveryBatchSize()));
        int finished = 0;
        for (Payment payment : stuck) {
            if (recover(payment)) {
                finished++;
            }
        }
        return finished;
    }

    private boolean recover(Payment payment) {
        Optional<ChargeResult> answer;
        try {
            answer = gateways.get(payment.getProvider()).lookup(payment.getGatewayKey());
        } catch (GatewayException providerTrouble) {
            // The provider can't tell us right now (down, slow, or its circuit breaker is open). Changing anything
            // without knowing could lose a paid order, so leave the payment PENDING and ask again next run.
            log.warn("Payment {} is still unresolved: {}", payment.getId(), providerTrouble.getMessage());
            count("still_unknown");
            return false;
        }
        // No record at all: the charge never arrived, so nothing was taken. Safe to give up on it.
        ChargeResult outcome = answer.orElse(ChargeResult.declined(NEVER_REACHED_PROVIDER));
        PaymentView result = tx.execute(status ->
                paymentService.recordOutcome(payment.getId(), payment.getHoldId(), outcome));
        log.info("Recovered payment {}: {} (hold {} is now {})", payment.getId(), result.paymentStatus(),
                result.holdId(), result.holdStatus());
        count(answer.isEmpty() ? "never_reached_provider" : outcome.succeeded() ? "succeeded" : "declined");
        return true;
    }

    private void count(String outcome) {
        meters.counter("flashsale.payments.recovered", "outcome", outcome).increment();
    }
}
