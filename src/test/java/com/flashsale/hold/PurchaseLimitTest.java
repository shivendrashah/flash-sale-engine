package com.flashsale.hold;

import static com.flashsale.support.Concurrently.failures;
import static com.flashsale.support.Concurrently.successes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;

import com.flashsale.common.IdempotentResult;
import com.flashsale.inventory.ConcurrencyStrategy;
import com.flashsale.inventory.ReservationMode;
import com.flashsale.inventory.StockLine;
import com.flashsale.support.Concurrently;
import com.flashsale.support.Concurrently.Outcome;
import com.flashsale.support.IntegrationTest;

/**
 * The per-user purchase limit (sales.per_user_limit, PurchaseLimits): a buyer can hold or buy up to the limit
 * and no more; ended holds give their units back; paid ones keep counting; the limit is per buyer and per
 * sale; and ten tabs of one buyer firing at once can't get past it, for every concurrency strategy.
 */
class PurchaseLimitTest extends IntegrationTest {

    private static final int TABS = 10;

    @Autowired
    HoldExpiryService expiryService;

    @Autowired
    PurchaseLimits purchaseLimits;

    // ===================== The rule =====================

    @Test
    void aBuyerCanHoldUpToTheLimitAndNoMore() {
        long item = limitedSaleItems(2, 10)[0];
        holdFor("alice", item, 1);
        holdFor("alice", item, 1);

        assertThatThrownBy(() -> holdFor("alice", item, 1))
                .isInstanceOf(PurchaseLimitExceededException.class)
                .hasMessageContaining("at most 2 unit(s) per buyer")
                .hasMessageContaining("You already hold or bought 2");
        assertThat(unitsHeld("alice", item)).isEqualTo(2);
        assertThat(availableStock(item)).isEqualTo(8);
        assertGateMatchesDatabase();
    }

    @Test
    void anAllOrNothingCartOverTheLimitIsRefusedBeforeAnythingIsTouched() {
        long[] ids = limitedSaleItems(2, 5, 5);

        assertThatThrownBy(() -> holdService.createHold(cart("alice", ReservationMode.ALL_OR_NOTHING,
                line(ids[0], 2), line(ids[1], 1))))
                .isInstanceOf(PurchaseLimitExceededException.class)
                .hasMessageContaining("needs at least 3 more");

        // Refused Redis was never asked, no stock row was touched
        assertThat(gateCounter(ids[0])).as("the Redis gate was never asked").isNull();
        assertThat(availableStock(ids[0])).isEqualTo(5);
        assertThat(count("user_sale_units")).isZero();
    }

    @Test
    void aPartialCartThatEndsUpOverTheLimitIsRefusedWholeAndGivesEverythingBack() {
        long[] ids = limitedSaleItems(3, 5, 5, 5, 0);   // the fourth item is sold out
        holdService.createHold(cart("alice", ReservationMode.ALL_OR_NOTHING, line(ids[0], 2)));   // alice has 2

        // PARTIAL could end with just 1 unit, so the early check lets it through. It takes 2 (the sold-out line is
        // skipped), and 2 + 2 > 3: the atomic check in the transaction refuses, and the ROLLBACK gives it all back.
        assertThatThrownBy(() -> holdService.createHold(cart("alice", ReservationMode.PARTIAL,
                line(ids[1], 1), line(ids[2], 1), line(ids[3], 1))))
                .isInstanceOf(PurchaseLimitExceededException.class)
                .hasMessageContaining("You already hold or bought 2, and this request needs at least 2 more");

        assertThat(availableStock(ids[1])).as("given back by the rollback").isEqualTo(5);
        assertThat(availableStock(ids[2])).isEqualTo(5);
        assertThat(unitsHeld("alice", ids[0])).as("alice's count is unchanged").isEqualTo(2);
        assertThat(count("holds")).isEqualTo(1);
        assertGateMatchesDatabase();   // the units Redis let through went back to Redis too
    }

    // ===================== What counts =====================

    @Test
    void cancelledAndExpiredHoldsGiveTheirUnitsBack() {
        long item = limitedSaleItems(1, 10)[0];
        HoldView first = holdFor("alice", item, 1);

        holdService.cancelHold("alice", first.holdId());
        assertThat(unitsHeld("alice", item)).isZero();
        holdFor("alice", item, 1);                            // allowed again

        clock.advance(Duration.ofMinutes(11));
        assertThat(expiryService.expireDueHolds()).isEqualTo(1);
        assertThat(unitsHeld("alice", item)).isZero();
        holdFor("alice", item, 1);                            // and again
    }

    @Test
    void paidHoldsKeepCounting() {
        long item = limitedSaleItems(1, 10)[0];
        HoldView hold = holdFor("alice", item, 1);
        assertThat(pay("alice", hold.holdId(), "tok_success").holdStatus()).isEqualTo(HoldStatus.CONFIRMED);

        assertThatThrownBy(() -> holdFor("alice", item, 1)).isInstanceOf(PurchaseLimitExceededException.class);
    }

    @Test
    void theLimitIsPerBuyerAndPerSale() {
        long firstSaleItem = limitedSaleItems(1, 10)[0];
        long secondSaleItem = limitedSaleItems(1, 10)[0];
        holdFor("alice", firstSaleItem, 1);

        assertThatThrownBy(() -> holdFor("alice", firstSaleItem, 1))
                .isInstanceOf(PurchaseLimitExceededException.class);
        holdFor("bob", firstSaleItem, 1);                     // another buyer, same sale
        holdFor("alice", secondSaleItem, 1);                  // same buyer, another sale
    }

    @Test
    void aSaleWithoutALimitCountsNothing() {
        long item = saleItemWithStock(10);                    // no perUserLimit

        for (int i = 0; i < 5; i++) {
            holdFor("alice", item, 1);
        }
        assertThat(count("user_sale_units")).isZero();
    }

    // ===================== Many tabs at once =====================

    @ParameterizedTest(name = "{0}: 10 tabs at once, a limit of 3, exactly 3 holds")
    @EnumSource(ConcurrencyStrategy.class)
    void tenTabsAtOnceCannotSneakPastTheLimit(ConcurrencyStrategy strategy) {
        long[] items = limitedSaleItems(3, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5);   // 10 different items

        List<Outcome<HoldView>> outcomes = Concurrently.run(TABS, tab -> holdService.createHold(new CreateHoldCommand(
                "alice", "tab-" + tab, strategy, List.of(new CreateHoldCommand.Item(items[tab], 1)))).body());

        assertThat(successes(outcomes)).isEqualTo(3);
        assertThat(failures(outcomes)).allSatisfy(error ->
                assertThat(error).isInstanceOf(PurchaseLimitExceededException.class));
        assertThat(unitsHeld("alice", items[0])).isEqualTo(3);
        assertThat(count("holds")).isEqualTo(3);
        assertGateMatchesDatabase();
    }

    @Test
    void aDoubleClickIsCountedOnce() {
        long item = limitedSaleItems(1, 10)[0];
        CreateHoldCommand doubleClick = holdCommand("alice", "double-click", item, 1);

        List<Outcome<IdempotentResult<HoldView>>> outcomes =
                Concurrently.run(TABS, copy -> holdService.createHold(doubleClick));

        assertThat(outcomes).as("every copy gets the hold, none is refused").allMatch(Outcome::succeeded);
        assertThat(outcomes.stream().map(outcome -> outcome.value().body().holdId()).distinct()).hasSize(1);
        assertThat(unitsHeld("alice", item)).isEqualTo(1);
    }

    // ===================== helpers =====================

    private int unitsHeld(String userId, long saleItemId) {
        long saleId = jdbc.queryForObject("SELECT sale_id FROM sale_items WHERE id = ?", Long.class, saleItemId);
        return purchaseLimits.unitsHeld(saleId, userId);
    }

    private static StockLine line(long saleItemId, int quantity) {
        return new StockLine(saleItemId, quantity);
    }

    private static CreateHoldCommand cart(String userId, ReservationMode mode, StockLine... lines) {
        return new CreateHoldCommand(userId, UUID.randomUUID().toString(), null, mode, Arrays.stream(lines)
                .map(line -> new CreateHoldCommand.Item(line.saleItemId(), line.quantity()))
                .toList());
    }
}
