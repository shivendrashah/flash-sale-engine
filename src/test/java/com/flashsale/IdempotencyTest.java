package com.flashsale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.flashsale.common.IdempotentResult;
import com.flashsale.common.error.IdempotencyKeyReusedException;
import com.flashsale.hold.CreateHoldCommand;
import com.flashsale.hold.HoldView;
import com.flashsale.hold.PaymentInProgressException;
import com.flashsale.payment.PayCommand;
import com.flashsale.payment.PaymentProvider;
import com.flashsale.payment.PaymentView;
import com.flashsale.support.Concurrently;
import com.flashsale.support.Concurrently.Outcome;
import com.flashsale.support.IntegrationTest;

/**
 * Retries and double-clicks must never take stock twice or charge a card twice, and every
 * copy of a double-click gets the same answer, even on the last unit and with the Redis stock gate on.
 */
class IdempotencyTest extends IntegrationTest {

    @Test
    void retryingWithTheSameKeyReturnsTheFirstHoldInsteadOfTakingMoreStock() {
        long saleItemId = saleItemWithStock(5);
        CreateHoldCommand command = holdCommand("alice", "key-1", saleItemId, 2);

        IdempotentResult<HoldView> first = holdService.createHold(command);
        IdempotentResult<HoldView> retry = holdService.createHold(command);   // same key, same request

        assertThat(first.replayed()).isFalse();
        assertThat(retry.replayed()).isTrue();
        assertThat(retry.body().holdId()).isEqualTo(first.body().holdId());
        assertThat(availableStock(saleItemId)).isEqualTo(3);   // 2 taken once, not 4
        assertThat(count("holds")).isEqualTo(1);
    }

    @Test
    void aRetryStillGetsItsHoldAfterTheItemSoldOut() {
        long saleItemId = saleItemWithStock(1);
        CreateHoldCommand command = holdCommand("alice", "key-1", saleItemId, 1);
        HoldView first = holdService.createHold(command).body();
        assertThat(availableStock(saleItemId)).isZero();

        IdempotentResult<HoldView> retry = holdService.createHold(command);

        assertThat(retry.replayed()).isTrue();
        assertThat(retry.body().holdId()).isEqualTo(first.holdId());
    }

    @Test
    void reusingAKeyForADifferentRequestIsRejected() {
        long saleItemId = saleItemWithStock(5);
        holdService.createHold(holdCommand("alice", "key-1", saleItemId, 1));

        assertThatThrownBy(() -> holdService.createHold(holdCommand("alice", "key-1", saleItemId, 2)))
                .isInstanceOf(IdempotencyKeyReusedException.class);
        assertThat(availableStock(saleItemId)).isEqualTo(4);
    }

    @Test
    void theSameRequestArrivingTenTimesAtOnceCreatesOneHold() {
        long saleItemId = saleItemWithStock(5);
        CreateHoldCommand doubleClick = holdCommand("alice", "double-click", saleItemId, 1);

        List<Outcome<IdempotentResult<HoldView>>> outcomes =
                Concurrently.run(10, attempt -> holdService.createHold(doubleClick));

        assertThat(outcomes).allMatch(Outcome::succeeded);
        assertThat(outcomes.stream().map(outcome -> outcome.value().body().holdId()).distinct()).hasSize(1);
        assertThat(outcomes.stream().filter(outcome -> !outcome.value().replayed())).hasSize(1);
        assertThat(availableStock(saleItemId)).isEqualTo(4);
        assertThat(count("holds")).isEqualTo(1);
        assertThat(gateCounter(saleItemId)).isEqualTo(4);
        assertGateMatchesDatabase();
    }

    @ParameterizedTest(name = "stock gate on: {0}")
    @ValueSource(booleans = {true, false})
    void aDoubleClickOnTheLastUnitGetsTheSameHoldForEveryCopy(boolean stockGate) {
        long saleItemId = saleItemWithStock(1);
        CreateHoldCommand doubleClick = holdCommand("alice", "double-click", saleItemId, 1).withStockGate(stockGate);

        List<Outcome<IdempotentResult<HoldView>>> outcomes =
                Concurrently.run(10, attempt -> holdService.createHold(doubleClick));

        assertThat(outcomes).as("no copy is told SOLD_OUT").allMatch(Outcome::succeeded);
        assertThat(outcomes.stream().map(outcome -> outcome.value().body().holdId()).distinct()).hasSize(1);
        assertThat(availableStock(saleItemId)).isZero();
        assertThat(count("holds")).isEqualTo(1);
        assertGateMatchesDatabase();
    }

    @Test
    void keysBelongToTheirUser() {
        long saleItemId = saleItemWithStock(5);

        holdService.createHold(holdCommand("alice", "key-1", saleItemId, 1));
        holdService.createHold(holdCommand("bob", "key-1", saleItemId, 1));

        assertThat(count("holds")).isEqualTo(2);
    }

    @Test
    void retryingAPaymentNeverChargesTwice() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 1);
        PayCommand command = new PayCommand("alice", hold.holdId(), "pay-1", PaymentProvider.RAZORPAY,
                "tok_success");

        IdempotentResult<PaymentView> first = paymentService.pay(command);
        IdempotentResult<PaymentView> retry = paymentService.pay(command);

        assertThat(retry.replayed()).isTrue();
        assertThat(retry.body().paymentId()).isEqualTo(first.body().paymentId());
        assertThat(retry.body().orderId()).isEqualTo(first.body().orderId());
        assertThat(retry.body().providerReference()).startsWith("pay_");  // FakeRazorpayGateway's id format
        assertThat(count("payments")).isEqualTo(1);
        assertThat(count("orders")).isEqualTo(1);
    }

    @Test
    void theSamePaymentSentFiveTimesAtOnceChargesOnce() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 1);
        PayCommand command = new PayCommand("alice", hold.holdId(), "pay-1", PaymentProvider.STRIPE, "tok_success");

        List<Outcome<IdempotentResult<PaymentView>>> outcomes =
                Concurrently.run(5, attempt -> paymentService.pay(command));

        // Each copy either did the payment, replayed its result, or was told "already in progress, retry".
        assertThat(outcomes).allSatisfy(outcome -> {
            if (!outcome.succeeded()) {
                assertThat(outcome.error()).isInstanceOf(PaymentInProgressException.class);
            }
        });
        assertThat(count("payments")).isEqualTo(1);
        assertThat(count("orders")).isEqualTo(1);
    }
}
