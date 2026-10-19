package com.flashsale.inventory;

import static com.flashsale.support.Concurrently.failures;
import static com.flashsale.support.Concurrently.successes;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.flashsale.catalog.SoldOutException;
import com.flashsale.hold.CreateHoldCommand;
import com.flashsale.hold.HoldView;
import com.flashsale.support.Concurrently;
import com.flashsale.support.Concurrently.Outcome;
import com.flashsale.support.IntegrationTest;

/**
 * The headline test: 50 buyers click "Buy" at the same instant.
 * First it proves the naive code oversells, then that every safe strategy doesn't. The Redis stock gate is
 * switched OFF here, so PostgreSQL alone decides; StockGateTest runs the same race with the gate on.
 */
class OversellTest extends IntegrationTest {

    private static final int BUYERS = 50;

    @Test
    void naiveReadCheckWriteSellsTheLastUnitSeveralTimes() {
        // Given: one unit left
        long saleItemId = saleItemWithStock(1);

        // When: 50 buyers click at the same instant, with the BROKEN strategy
        List<Outcome<HoldView>> outcomes = everyoneBuysAtOnce(saleItemId, ConcurrencyStrategy.NAIVE);

        // Then: several of them got it. This proves the test CAN catch the bug (a "control group").
        assertThat(successes(outcomes)).as("buyers who got the only unit").isGreaterThan(1);
        assertThat(availableStock(saleItemId)).as("the stock column still looks fine...").isZero();
        assertThat(unitsHeldOrSold(saleItemId)).as("...but more units are held than ever existed").isGreaterThan(1);
    }

    @ParameterizedTest(name = "{0}: the last unit goes to exactly one buyer")
    @EnumSource(value = ConcurrencyStrategy.class, names = "NAIVE", mode = EnumSource.Mode.EXCLUDE)
    void safeStrategiesSellTheLastUnitExactlyOnce(ConcurrencyStrategy strategy) {
        long saleItemId = saleItemWithStock(1);

        List<Outcome<HoldView>> outcomes = everyoneBuysAtOnce(saleItemId, strategy);

        assertThat(successes(outcomes)).isEqualTo(1);
        assertThat(failures(outcomes)).allSatisfy(error ->
                assertThat(error).isInstanceOfAny(SoldOutException.class, StockBusyException.class));
        assertThat(availableStock(saleItemId)).isZero();
        assertThat(unitsHeldOrSold(saleItemId)).isEqualTo(1);
    }

    @ParameterizedTest(name = "{0}: 10 units, 50 buyers, never oversold")
    @EnumSource(value = ConcurrencyStrategy.class, names = "NAIVE", mode = EnumSource.Mode.EXCLUDE)
    void safeStrategiesNeverOversell(ConcurrencyStrategy strategy) {
        long saleItemId = saleItemWithStock(10);

        List<Outcome<HoldView>> outcomes = everyoneBuysAtOnce(saleItemId, strategy);

        long sold = successes(outcomes);
        assertThat(sold).isBetween(1L, 10L);
        assertThat(unitsHeldOrSold(saleItemId)).isEqualTo((int) sold);
        // The invariant: every unit is either still on sale or held by exactly one buyer.
        assertThat(availableStock(saleItemId) + unitsHeldOrSold(saleItemId))
                .as("every unit is either still on sale or held by exactly one buyer")
                .isEqualTo(10);
        if (strategy != ConcurrencyStrategy.OPTIMISTIC) {
            // Lock-based strategies make buyers queue, so nobody gives up while stock is left.
            // OPTIMISTIC may let some buyers run out of retries first: correct, but it can under-sell.
            assertThat(sold).isEqualTo(10);
        }
    }

    private List<Outcome<HoldView>> everyoneBuysAtOnce(long saleItemId, ConcurrencyStrategy strategy) {
        // Buyer n is user "buyer-n" with Idempotency-Key "key-n": 50 different users, 50 different requests.
        return Concurrently.run(BUYERS, buyer -> holdService.createHold(new CreateHoldCommand(
                "buyer-" + buyer, "key-" + buyer, strategy,
                List.of(new CreateHoldCommand.Item(saleItemId, 1))).withStockGate(false)).body());
    }
}
