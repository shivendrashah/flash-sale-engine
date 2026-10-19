package com.flashsale.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.common.Pause;
import com.flashsale.hold.HoldExpiryService;
import com.flashsale.hold.HoldStatus;
import com.flashsale.hold.HoldView;
import com.flashsale.hold.PaymentInProgressException;
import com.flashsale.payment.gateway.ChargeResult;
import com.flashsale.support.IntegrationTest;

/**
 * Payment safety: a provider that answers too late, one that loses the request, one that is down, and the
 * circuit breaker and recovery job that deal with them. The fake gateway's test tokens play each failure:
 *   tok_timeout      charged, but the answer comes after the 200 ms test timeout
 *   tok_lost         never reaches the provider; we time out
 *   tok_unavailable  the provider answers 503 at once; nothing charged
 */
class PaymentResilienceTest extends IntegrationTest {

    @Autowired
    PaymentRecoveryService recovery;

    @Autowired
    HoldExpiryService expiryService;

    @Autowired
    TransactionTemplate tx;

    @Autowired
    MeterRegistry meters;

    // ===================== Ending 4: no answer in time =====================

    @Test
    void aProviderThatAnswersTooLateLeavesThePaymentPendingAndTheHoldClaimed() {
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);
        PayCommand command = payCommand("alice", hold.holdId(), "pay-1", "tok_timeout");

        long started = System.nanoTime();
        PaymentView payment = paymentService.pay(command).body();
        long waitedMs = (System.nanoTime() - started) / 1_000_000;

        assertThat(payment.paymentStatus()).as("not FAILED: the card may have been charged")
                .isEqualTo(PaymentStatus.PENDING);
        assertThat(payment.holdStatus()).isEqualTo(HoldStatus.PAYMENT_PENDING);
        assertThat(waitedMs).as("gave up after the 200 ms timeout").isBetween(200L, 2_000L);
        // The same request again: still in progress (409), and the card is never charged a second time
        assertThatThrownBy(() -> paymentService.pay(command)).isInstanceOf(PaymentInProgressException.class);
        // A hold with a payment in flight is never expired, even long after its expiry time
        clock.advance(Duration.ofMinutes(30));
        assertThat(expiryService.expireDueHolds()).isZero();
    }

    // ===================== the recovery job =====================

    @Test
    void theRecoveryJobConfirmsAPaymentTheProviderDidCharge() {
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);
        PayCommand command = payCommand("alice", hold.holdId(), "pay-1", "tok_timeout");
        paymentService.pay(command);                         // 202: PENDING

        clock.advance(Duration.ofMinutes(2));               // older than recover-after (1 minute): stuck
        assertThat(recovery.recoverStuckPayments()).isEqualTo(1);

        // The same request again now gets the final answer
        PaymentView paid = paymentService.pay(command).body();
        assertThat(paid.paymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(paid.holdStatus()).isEqualTo(HoldStatus.CONFIRMED);
        assertThat(paid.providerReference()).as("the charge the provider made").startsWith("pi_");
        assertThat(paid.orderId()).isNotNull();
        assertThat(count("orders")).isEqualTo(1);
    }

    @Test
    void theRecoveryJobFailsAPaymentTheProviderNeverReceived() {
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);
        PayCommand command = payCommand("alice", hold.holdId(), "pay-1", "tok_lost");
        paymentService.pay(command);

        clock.advance(Duration.ofMinutes(2));
        assertThat(recovery.recoverStuckPayments()).isEqualTo(1);

        PaymentView failed = paymentService.pay(command).body();
        assertThat(failed.paymentStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(failed.failureReason()).isEqualTo("never_reached_provider");
        assertThat(failed.holdStatus()).as("the hold is free again").isEqualTo(HoldStatus.HELD);
        // Nothing was charged, so the buyer simply pays again (a new attempt, a new key)
        assertThat(pay("alice", hold.holdId(), "tok_success").holdStatus()).isEqualTo(HoldStatus.CONFIRMED);
    }

    @Test
    void paymentsThatMayStillBeRunningAreLeftAlone() {
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);
        paymentService.pay(payCommand("alice", hold.holdId(), "pay-1", "tok_timeout"));

        clock.advance(Duration.ofSeconds(30));              // younger than recover-after

        assertThat(recovery.recoverStuckPayments()).isZero();
    }

    @Test
    void whileTheProviderCannotAnswerTheRecoveryJobChangesNothingAndTriesAgainLater() {
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);
        PayCommand command = payCommand("alice", hold.holdId(), "pay-1", "tok_timeout");
        paymentService.pay(command);
        clock.advance(Duration.ofMinutes(2));

        circuitBreakers.circuitBreaker("STRIPE").transitionToForcedOpenState();   // Stripe is unreachable
        assertThat(recovery.recoverStuckPayments()).isZero();
        assertThatThrownBy(() -> paymentService.pay(command)).isInstanceOf(PaymentInProgressException.class);

        circuitBreakers.circuitBreaker("STRIPE").transitionToClosedState();       // Stripe is back
        assertThat(recovery.recoverStuckPayments()).isEqualTo(1);
        assertThat(paymentService.pay(command).body().holdStatus()).isEqualTo(HoldStatus.CONFIRMED);
    }

    // ===================== Ending 3: the provider is down =====================

    @Test
    void aProviderOutageChargesNothingAndLeavesNoTrace() {
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);
        PayCommand command = payCommand("alice", hold.holdId(), "pay-1", "tok_unavailable");

        assertThatThrownBy(() -> paymentService.pay(command))
                .isInstanceOfSatisfying(PaymentProviderUnavailableException.class,
                        error -> assertThat(error.retryAfterSeconds()).isEqualTo(1));   // 500 ms, rounded up

        assertThat(count("payments")).as("the attempt was undone").isZero();
        assertThat(holdService.getHold("alice", hold.holdId()).status()).isEqualTo(HoldStatus.HELD);
        // Nothing was remembered under the key, so the same request really tries again (it is not a replay)
        assertThatThrownBy(() -> paymentService.pay(command)).isInstanceOf(PaymentProviderUnavailableException.class);
    }

    @Test
    void afterRepeatedFailuresTheCircuitOpensAndPaymentsFailFastWithoutReachingTheProvider() {
        long saleItemId = saleItemWithStock(20);
        CircuitBreaker stripe = circuitBreakers.circuitBreaker("STRIPE");
        // 5 calls (the minimum to judge), all answered 503 by the provider: 100% failed >= 50%, so OPEN
        for (int i = 0; i < 5; i++) {
            String buyer = "buyer-" + i;
            HoldView hold = holdFor(buyer, saleItemId, 1);
            assertThatThrownBy(() -> pay(buyer, hold.holdId(), "tok_unavailable"))
                    .isInstanceOf(PaymentProviderUnavailableException.class);
        }
        assertThat(stripe.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Now even a good card fails at once: the call is never sent to the provider
        HoldView alice = holdFor("alice", saleItemId, 1);
        PayCommand command = payCommand("alice", alice.holdId(), "alice-pay", "tok_success");
        assertThatThrownBy(() -> paymentService.pay(command)).isInstanceOf(PaymentProviderUnavailableException.class);
        assertThat(stripe.getMetrics().getNumberOfNotPermittedCalls()).as("refused by the breaker").isEqualTo(1);
        assertThat(holdService.getHold("alice", alice.holdId()).status()).isEqualTo(HoldStatus.HELD);

        // After wait-in-open-state (500 ms in tests) the breaker lets trial calls through, and the SAME request works
        Pause.of(Duration.ofMillis(600));
        assertThat(paymentService.pay(command).body().paymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(stripe.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
        // A second good trial call (2 are permitted) closes the circuit again
        HoldView bob = holdFor("bob", saleItemId, 1);
        pay("bob", bob.holdId(), "tok_success");
        assertThat(stripe.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void oneProviderBeingDownDoesNotStopTheOther() {
        circuitBreakers.circuitBreaker("STRIPE").transitionToForcedOpenState();
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);

        PaymentView paid = paymentService.pay(new PayCommand("alice", hold.holdId(), "pay-1",
                PaymentProvider.RAZORPAY, "tok_success")).body();

        assertThat(paid.paymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(paid.providerReference()).startsWith("pay_");
    }

    @Test
    void declinedCardsDoNotOpenTheCircuit() {
        long saleItemId = saleItemWithStock(20);
        for (int i = 0; i < 10; i++) {
            HoldView hold = holdFor("buyer-" + i, saleItemId, 1);
            assertThat(pay("buyer-" + i, hold.holdId(), "tok_declined").paymentStatus())
                    .isEqualTo(PaymentStatus.FAILED);
        }
        // A decline is a normal answer from a healthy provider, not a provider failure
        assertThat(circuitBreakers.circuitBreaker("STRIPE").getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    // ===================== Two finishers for one payment =====================

    @Test
    void finishingAPaymentTwiceChangesNothingTheSecondTime() {
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);
        PaymentView pending = paymentService.pay(payCommand("alice", hold.holdId(), "pay-1", "tok_timeout")).body();
        clock.advance(Duration.ofMinutes(2));
        recovery.recoverStuckPayments();                     // SUCCEEDED, hold CONFIRMED, order created

        // A request that stalled for minutes finally hears "declined" for the same payment: too late
        PaymentView late = tx.execute(status -> paymentService.recordOutcome(pending.paymentId(), hold.holdId(),
                ChargeResult.declined("card_declined")));

        assertThat(late.paymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(late.holdStatus()).isEqualTo(HoldStatus.CONFIRMED);
        assertThat(count("orders")).isEqualTo(1);
    }

    @Test
    void aChargeThatSucceedsAfterThePaymentWasGivenUpIsFlaggedForARefund() {
        HoldView hold = holdFor("alice", saleItemWithStock(5), 1);
        PaymentView pending = paymentService.pay(payCommand("alice", hold.holdId(), "pay-1", "tok_lost")).body();
        clock.advance(Duration.ofMinutes(2));
        recovery.recoverStuckPayments();                     // FAILED: the provider had no record of it
        double before = refundsNeeded();

        // ...and then a stalled request's charge goes through after all
        PaymentView late = tx.execute(status -> paymentService.recordOutcome(pending.paymentId(), hold.holdId(),
                ChargeResult.approved("pi_late")));

        assertThat(late.paymentStatus()).as("the record is not rewritten").isEqualTo(PaymentStatus.FAILED);
        assertThat(refundsNeeded() - before).as("but the money taken is flagged").isEqualTo(1);
    }

    // ===================== helpers =====================

    private static PayCommand payCommand(String userId, long holdId, String idempotencyKey, String paymentToken) {
        return new PayCommand(userId, holdId, idempotencyKey, PaymentProvider.STRIPE, paymentToken);
    }

    private double refundsNeeded() {
        Counter counter = meters.find("flashsale.payments.needs_refund").counter();
        return counter == null ? 0 : counter.count();
    }
}
