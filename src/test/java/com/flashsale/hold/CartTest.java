package com.flashsale.hold;

import static com.flashsale.support.Concurrently.failures;
import static com.flashsale.support.Concurrently.successes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;

import com.flashsale.catalog.SoldOutException;
import com.flashsale.common.IdempotentResult;
import com.flashsale.common.error.IdempotencyKeyReusedException;
import com.flashsale.inventory.ConcurrencyStrategy;
import com.flashsale.inventory.ReservationMode;
import com.flashsale.inventory.StockBusyException;
import com.flashsale.inventory.StockLine;
import com.flashsale.payment.PaymentView;
import com.flashsale.support.Concurrently;
import com.flashsale.support.Concurrently.Outcome;
import com.flashsale.support.IntegrationTest;

/**
 * The Phase 2 cart: several items in one hold, ALL_OR_NOTHING vs PARTIAL, for every concurrency strategy.
 * It proves PLAN.md's "done when" for the cart: ALL_OR_NOTHING never leaves a half-reserved cart; PARTIAL holds
 * exactly what is available and reports the rest; no deadlocks and no oversells when many carts overlap.
 * It runs with the Redis stock gate ON (the default), and checks after every test that the gate's counters
 * still equal PostgreSQL's stock. CartWithoutGateTest runs every test again with the gate OFF.
 */
class CartTest extends IntegrationTest {

    private static final int BUYERS = 40;

    @Autowired
    HoldExpiryService expiryService;

    /** null = the Redis stock gate follows application.yml (on). CartWithoutGateTest overrides this with false. */
    protected Boolean stockGate() {
        return null;
    }

    @AfterEach
    void redisGateStillAgreesWithPostgres() {
        assertGateMatchesDatabase();
    }

    // ===================== One buyer: what each policy does with a short item =====================
    // Run for EVERY strategy, NAIVE included: one buyer on their own can't trigger NAIVE's race.

    @ParameterizedTest(name = "{0}")
    @EnumSource(ConcurrencyStrategy.class)
    void allOrNothingHoldsEveryItemWhenAllAreInStock(ConcurrencyStrategy strategy) {
        long[] ids = saleItemsWithStock(5, 5);  // item 1 costs 100.00, item 2 costs 200.00

        // listed in reverse order on purpose: the hold still comes back in id order
        HoldView hold = cart("alice", strategy, ReservationMode.ALL_OR_NOTHING, line(ids[1], 1), line(ids[0], 2));

        assertThat(hold.items()).extracting(HoldView.Item::saleItemId).containsExactly(ids[0], ids[1]);
        assertThat(hold.totalAmount()).isEqualByComparingTo("400.00");   // 2 x 100.00 + 1 x 200.00
        assertThat(hold.unavailable()).isEmpty();
        assertThat(availableStock(ids[0])).isEqualTo(3);
        assertThat(availableStock(ids[1])).isEqualTo(4);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ConcurrencyStrategy.class)
    void allOrNothingHoldsNothingIfOneItemIsShort(ConcurrencyStrategy strategy) {
        long[] ids = saleItemsWithStock(5, 0);

        assertThatThrownBy(() -> cart("alice", strategy, ReservationMode.ALL_OR_NOTHING,
                line(ids[0], 1), line(ids[1], 1)))
                .isInstanceOf(SoldOutException.class)
                .hasMessageContaining("Sale item " + ids[1]);      // names the item that was short
        assertThat(availableStock(ids[0])).as("the in-stock item is still on sale").isEqualTo(5);
        assertThat(count("holds")).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ConcurrencyStrategy.class)
    void partialHoldsWhatIsInStockAndReportsTheRest(ConcurrencyStrategy strategy) {
        long[] ids = saleItemsWithStock(5, 0, 2);

        HoldView hold = cart("alice", strategy, ReservationMode.PARTIAL,
                line(ids[0], 1), line(ids[1], 1), line(ids[2], 3));

        assertThat(hold.items()).extracting(HoldView.Item::saleItemId).containsExactly(ids[0]);
        assertThat(hold.totalAmount()).as("only what was held is charged").isEqualByComparingTo("100.00");
        assertThat(hold.unavailable()).extracting(HoldView.UnavailableItem::saleItemId).containsExactly(ids[1], ids[2]);
        assertThat(availableStock(ids[0])).isEqualTo(4);
        assertThat(availableStock(ids[2])).as("2 left is not enough for 3, so it is untouched").isEqualTo(2);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ConcurrencyStrategy.class)
    void partialWithNothingInStockIsSoldOut(ConcurrencyStrategy strategy) {
        long[] ids = saleItemsWithStock(0, 0);

        assertThatThrownBy(() -> cart("alice", strategy, ReservationMode.PARTIAL, line(ids[0], 1), line(ids[1], 1)))
                .isInstanceOf(SoldOutException.class)
                .hasMessageContaining("None of the 2");
        assertThat(count("holds")).as("no empty hold").isZero();
    }

    // ===================== The rest of a hold's life covers every item in it =====================

    @Test
    void anExpiredCartGivesEveryItemBack() {
        long[] ids = saleItemsWithStock(5, 5);
        cart("alice", null, ReservationMode.ALL_OR_NOTHING, line(ids[0], 2), line(ids[1], 1));

        clock.advance(Duration.ofMinutes(11));

        assertThat(expiryService.expireDueHolds()).isEqualTo(1);
        assertThat(availableStock(ids[0])).isEqualTo(5);
        assertThat(availableStock(ids[1])).isEqualTo(5);
    }

    @Test
    void aCancelledCartGivesEveryItemBack() {
        long[] ids = saleItemsWithStock(5, 5);
        HoldView hold = cart("alice", null, ReservationMode.ALL_OR_NOTHING, line(ids[0], 2), line(ids[1], 1));

        holdService.cancelHold("alice", hold.holdId());

        assertThat(availableStock(ids[0])).isEqualTo(5);
        assertThat(availableStock(ids[1])).isEqualTo(5);
    }

    @Test
    void payingForACartChargesTheTotalAndConfirmsEveryItem() {
        long[] ids = saleItemsWithStock(5, 5);
        HoldView hold = cart("alice", null, ReservationMode.ALL_OR_NOTHING, line(ids[0], 2), line(ids[1], 1));

        PaymentView payment = pay("alice", hold.holdId(), "tok_success");

        assertThat(payment.holdStatus()).isEqualTo(HoldStatus.CONFIRMED);
        assertThat(payment.amount()).isEqualByComparingTo("400.00");
        assertThat(unitsHeldOrSold(ids[0])).isEqualTo(2);
        assertThat(unitsHeldOrSold(ids[1])).isEqualTo(1);
    }

    @Test
    void itemsFromDifferentSalesCannotShareAHold() {
        long first = saleItemWithStock(5);    // each call creates its own sale
        long second = saleItemWithStock(5);

        assertThatThrownBy(() -> cart("alice", null, ReservationMode.ALL_OR_NOTHING, line(first, 1), line(second, 1)))
                .isInstanceOf(MixedSalesException.class);
        assertThat(availableStock(first)).isEqualTo(5);
    }

    @Test
    void aRetriedPartialCartGetsTheSameAnswerAndTakesStockOnce() {
        long[] ids = saleItemsWithStock(5, 0);
        CreateHoldCommand command = cartCommand("alice", "cart-1", null, ReservationMode.PARTIAL,
                List.of(line(ids[0], 1), line(ids[1], 1)));

        IdempotentResult<HoldView> first = holdService.createHold(command);
        IdempotentResult<HoldView> retry = holdService.createHold(command);

        assertThat(retry.replayed()).isTrue();
        assertThat(retry.body().holdId()).isEqualTo(first.body().holdId());
        assertThat(retry.body().unavailable()).as("the replay repeats the skipped items too")
                .isEqualTo(first.body().unavailable())
                .extracting(HoldView.UnavailableItem::saleItemId).containsExactly(ids[1]);
        assertThat(availableStock(ids[0])).isEqualTo(4);
    }

    @Test
    void theSameItemsInAnotherModeIsADifferentRequest() {
        long[] ids = saleItemsWithStock(5, 5);
        List<StockLine> lines = List.of(line(ids[0], 1), line(ids[1], 1));
        holdService.createHold(cartCommand("alice", "cart-1", null, ReservationMode.ALL_OR_NOTHING, lines));

        // The mode is part of the request fingerprint: same key + same items + another mode = a reused key (422).
        assertThatThrownBy(() -> holdService.createHold(
                cartCommand("alice", "cart-1", null, ReservationMode.PARTIAL, lines)))
                .isInstanceOf(IdempotencyKeyReusedException.class);
    }

    // ===================== Many buyers at once =====================

    @ParameterizedTest(name = "{0}: opposite item order, no deadlock, no oversell")
    @EnumSource(value = ConcurrencyStrategy.class, names = "NAIVE", mode = EnumSource.Mode.EXCLUDE)
    void cartsWithTheSameItemsInOppositeOrderNeverDeadlock(ConcurrencyStrategy strategy) {
        long[] ids = saleItemsWithStock(10, 10);

        // Even buyers list [A, B], odd buyers [B, A]. If each transaction locked rows in the order it was given,
        // two of them would each hold one row and wait forever for the other's: a deadlock.
        List<Outcome<HoldView>> outcomes = Concurrently.run(BUYERS, buyer -> holdService.createHold(cartCommand(
                "buyer-" + buyer, "key-" + buyer, strategy, ReservationMode.ALL_OR_NOTHING,
                buyer % 2 == 0 ? List.of(line(ids[0], 1), line(ids[1], 1))
                               : List.of(line(ids[1], 1), line(ids[0], 1)))).body());

        // A deadlock would show up here as a different exception (PostgreSQL's "deadlock detected").
        assertThat(failures(outcomes)).allSatisfy(error ->
                assertThat(error).isInstanceOfAny(SoldOutException.class, StockBusyException.class));
        long sold = successes(outcomes);
        assertThat(sold).isBetween(1L, 10L);
        for (long id : ids) {
            assertThat(unitsHeldOrSold(id)).as("every hold has both items, never just one").isEqualTo((int) sold);
            assertThat(availableStock(id) + unitsHeldOrSold(id)).as("no unit lost or sold twice").isEqualTo(10);
        }
    }

    @ParameterizedTest(name = "{0}: PARTIAL carts never oversell")
    @EnumSource(value = ConcurrencyStrategy.class, names = "NAIVE", mode = EnumSource.Mode.EXCLUDE)
    void partialCartsNeverOversell(ConcurrencyStrategy strategy) {
        int[] stock = {3, 7};
        long[] ids = saleItemsWithStock(stock);

        List<Outcome<HoldView>> outcomes = Concurrently.run(BUYERS, buyer -> holdService.createHold(cartCommand(
                "buyer-" + buyer, "key-" + buyer, strategy, ReservationMode.PARTIAL,
                List.of(line(ids[0], 1), line(ids[1], 1)))).body());

        assertThat(failures(outcomes)).allSatisfy(error ->
                assertThat(error).isInstanceOfAny(SoldOutException.class, StockBusyException.class));
        for (int i = 0; i < ids.length; i++) {
            assertThat(availableStock(ids[i]) + unitsHeldOrSold(ids[i]))
                    .as("every unit is either still on sale or held by exactly one buyer")
                    .isEqualTo(stock[i]);
        }
        if (strategy != ConcurrencyStrategy.OPTIMISTIC) {
            // Lock-based strategies make buyers queue, so everything sells: 3 carts get both items, the next 4 get
            // only the second one, and the rest are SOLD_OUT. (OPTIMISTIC buyers may give up first: correct, but it
            // can under-sell, as in OversellTest.)
            assertThat(availableStock(ids[0])).isZero();
            assertThat(availableStock(ids[1])).isZero();
        }
    }

    // ===================== helpers =====================

    private static StockLine line(long saleItemId, int quantity) {
        return new StockLine(saleItemId, quantity);
    }

    /** Creates a hold for a cart with a fresh Idempotency-Key and returns it. strategy null = the default. */
    private HoldView cart(String userId, ConcurrencyStrategy strategy, ReservationMode mode, StockLine... lines) {
        return holdService.createHold(
                cartCommand(userId, UUID.randomUUID().toString(), strategy, mode, List.of(lines))).body();
    }

    private CreateHoldCommand cartCommand(String userId, String idempotencyKey, ConcurrencyStrategy strategy,
                                          ReservationMode mode, List<StockLine> lines) {
        return new CreateHoldCommand(userId, idempotencyKey, strategy, stockGate(), mode, lines.stream()
                .map(line -> new CreateHoldCommand.Item(line.saleItemId(), line.quantity()))
                .toList());
    }
}
