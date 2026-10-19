package com.flashsale.hold;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.flashsale.catalog.CreateSaleRequest;
import com.flashsale.catalog.SaleNotActiveException;
import com.flashsale.catalog.SaleView;
import com.flashsale.catalog.SoldOutException;
import com.flashsale.common.error.NotFoundException;
import com.flashsale.payment.PaymentStatus;
import com.flashsale.payment.PaymentView;
import com.flashsale.support.IntegrationTest;

/**
 * The normal life of a hold, end to end through the real services and a real database:
 * hold -> pay, declined card -> retry, cancel, and the rules around them.
 */
class HoldLifecycleTest extends IntegrationTest {

    @Test
    void payingForAHoldConfirmsItAndCreatesAnOrder() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 2);

        PaymentView payment = pay("alice", hold.holdId(), "tok_success");

        assertThat(payment.paymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(payment.holdStatus()).isEqualTo(HoldStatus.CONFIRMED);
        assertThat(payment.orderId()).isNotNull();
        assertThat(payment.amount()).isEqualByComparingTo("119998.00"); // 2 x 59999.00
        assertThat(payment.providerReference()).startsWith("pi_");     // FakeStripeGateway's id format
        assertThat(availableStock(saleItemId)).as("sold units stay out of stock").isEqualTo(3);
    }

    @Test
    void aDeclinedCardKeepsTheHoldSoTheUserCanTryAgain() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 1);

        PaymentView declined = pay("alice", hold.holdId(), "tok_declined");

        assertThat(declined.paymentStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(declined.failureReason()).isEqualTo("card_declined");
        assertThat(declined.holdStatus()).isEqualTo(HoldStatus.HELD);
        assertThat(availableStock(saleItemId)).as("the unit is still kept aside for alice").isEqualTo(4);

        PaymentView retried = pay("alice", hold.holdId(), "tok_success");

        assertThat(retried.paymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(retried.holdStatus()).isEqualTo(HoldStatus.CONFIRMED);
    }

    @Test
    void cancellingAHoldPutsItsStockBackOnSale() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 3);
        assertThat(availableStock(saleItemId)).isEqualTo(2);

        HoldView cancelled = holdService.cancelHold("alice", hold.holdId());

        assertThat(cancelled.status()).isEqualTo(HoldStatus.CANCELLED);
        assertThat(availableStock(saleItemId)).isEqualTo(5);
    }

    @Test
    void aPaidHoldCannotBeCancelledOrPaidAgain() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 1);
        pay("alice", hold.holdId(), "tok_success");

        assertThatThrownBy(() -> holdService.cancelHold("alice", hold.holdId()))
                .isInstanceOf(IllegalHoldTransitionException.class);
        assertThatThrownBy(() -> pay("alice", hold.holdId(), "tok_success"))
                .isInstanceOf(IllegalHoldTransitionException.class);
        assertThat(availableStock(saleItemId)).isEqualTo(4);
        assertThat(count("orders")).isEqualTo(1);
    }

    @Test
    void usersCannotSeeOrTouchSomeoneElsesHold() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 1);

        assertThatThrownBy(() -> holdService.getHold("mallory", hold.holdId())).isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> holdService.cancelHold("mallory", hold.holdId()))
                .isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> pay("mallory", hold.holdId(), "tok_success")).isInstanceOf(NotFoundException.class);
    }

    @Test
    void holdsCanOnlyBeCreatedWhileTheSaleIsRunning() {
        Instant now = clock.instant();
        SaleView tomorrow = saleService.createSale(new CreateSaleRequest("Tomorrow's sale",
                now.plus(Duration.ofDays(1)), now.plus(Duration.ofDays(2)), List.of(iphone(5))));
        long saleItemId = tomorrow.items().getFirst().saleItemId();

        assertThatThrownBy(() -> holdFor("alice", saleItemId, 1)).isInstanceOf(SaleNotActiveException.class);
    }

    @Test
    void askingForMoreThanIsLeftIsRefusedAndTakesNothing() {
        long saleItemId = saleItemWithStock(2);

        assertThatThrownBy(() -> holdFor("alice", saleItemId, 3)).isInstanceOf(SoldOutException.class);
        assertThat(availableStock(saleItemId)).isEqualTo(2);
        assertThat(count("holds")).isZero(); // the transaction rolled back: nothing half-done was saved
    }
}
