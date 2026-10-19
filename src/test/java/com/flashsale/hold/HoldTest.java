package com.flashsale.hold;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Plain unit test of the Hold entity's rules. No database: the rules live in the entity itself, so we can
 * test them on a plain Java object (one benefit of a "rich domain model").
 */
class HoldTest {

    private final Instant now = Instant.parse("2026-10-02T10:00:00Z");

    private Hold newHold() {
        return Hold.create("alice", 1L, List.of(new HoldItem.Line(7L, 2, new BigDecimal("100.00"))), now,
                Duration.ofMinutes(10), "key-1", "hash-1");
    }

    @Test
    void theTotalIsQuantityTimesThePriceAtHoldTime() {
        // isEqualByComparingTo: compares BigDecimal values (200.00 == 200.0), unlike equals()
        assertThat(newHold().getTotalAmount()).isEqualByComparingTo("200.00");
    }

    @Test
    void paymentCanStartBeforeTheHoldRunsOut() {
        Hold hold = newHold();
        hold.startPayment(now.plus(Duration.ofMinutes(9)));
        assertThat(hold.getStatus()).isEqualTo(HoldStatus.PAYMENT_PENDING);
    }

    @Test
    void paymentCannotStartOnceTimeIsUp() {
        Hold hold = newHold();
        assertThatThrownBy(() -> hold.startPayment(now.plus(Duration.ofMinutes(10))))
                .isInstanceOf(HoldExpiredException.class);
    }

    @Test
    void aSecondPaymentCannotStartWhileOneIsRunning() {
        Hold hold = newHold();
        hold.startPayment(now);
        assertThatThrownBy(() -> hold.startPayment(now)).isInstanceOf(PaymentInProgressException.class);
    }

    @Test
    void aDeclinedPaymentPutsTheHoldBackToHeld() {
        Hold hold = newHold();
        hold.startPayment(now);
        hold.paymentDeclined(now);
        assertThat(hold.getStatus()).isEqualTo(HoldStatus.HELD);
    }

    @Test
    void aHoldCannotExpireEarly() {
        Hold hold = newHold();
        assertThatThrownBy(() -> hold.expire(now.plus(Duration.ofMinutes(5))))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aConfirmedHoldCannotBeCancelled() {
        Hold hold = newHold();
        hold.startPayment(now);
        hold.confirm(now);
        assertThatThrownBy(() -> hold.cancel(now)).isInstanceOf(IllegalHoldTransitionException.class);
    }
}
