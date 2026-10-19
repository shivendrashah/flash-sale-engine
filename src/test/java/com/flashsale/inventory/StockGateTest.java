package com.flashsale.inventory;

import static com.flashsale.support.Concurrently.failures;
import static com.flashsale.support.Concurrently.successes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.catalog.SoldOutException;
import com.flashsale.common.IdempotentResult;
import com.flashsale.hold.CreateHoldCommand;
import com.flashsale.hold.HoldExpiryService;
import com.flashsale.hold.HoldStatus;
import com.flashsale.hold.HoldView;
import com.flashsale.support.Concurrently;
import com.flashsale.support.Concurrently.Outcome;
import com.flashsale.support.IntegrationTest;

/**
 * The Redis stock gate (StockGate):
 *   1. it turns the crowd away without touching PostgreSQL,
 *   2. it never decides a sale on its own: when Redis is wrong, PostgreSQL still decides,
 *   3. its counters stay equal to PostgreSQL's stock through carts, rollbacks, cancels, expiries and payments.
 */
class StockGateTest extends IntegrationTest {

    private static final int BUYERS = 50;

    @Autowired
    MeterRegistry meters;

    @Autowired
    HoldExpiryService expiryService;

    @Autowired
    StockReleaser stockReleaser;

    @Autowired
    TransactionTemplate tx;

    // ===================== 1. The crowd never reaches PostgreSQL =====================

    @ParameterizedTest(name = "{0}: Redis lets one buyer through, and one unit is sold")
    @EnumSource(ConcurrencyStrategy.class)
    void withTheGateTheLastUnitGoesToExactlyOneBuyer(ConcurrencyStrategy strategy) {
        long saleItemId = saleItemWithStock(1);
        double refusedBefore = gateCount("refused");

        List<Outcome<HoldView>> outcomes = everyoneBuysAtOnce(saleItemId, strategy);

        assertThat(successes(outcomes)).isEqualTo(1);
        assertThat(failures(outcomes)).allSatisfy(error -> assertThat(error).isInstanceOf(SoldOutException.class));
        assertThat(gateCount("refused") - refusedBefore)
                .as("buyers Redis turned away: they never reached the stock row")
                .isEqualTo(BUYERS - 1);
        assertThat(unitsHeldOrSold(saleItemId)).isEqualTo(1);
        assertThat(gateCounter(saleItemId)).isZero();
        assertGateMatchesDatabase();
    }

    // ===================== 2. Redis can say no; only PostgreSQL can say yes =====================

    @ParameterizedTest(name = "{0}: Redis says 50 left, PostgreSQL has 1, exactly 1 is sold")
    @EnumSource(value = ConcurrencyStrategy.class, names = "NAIVE", mode = EnumSource.Mode.EXCLUDE)
    void whenRedisSaysMoreThanThereIsPostgresStillDecides(ConcurrencyStrategy strategy) {
        long saleItemId = saleItemWithStock(1);
        setGateCounter(saleItemId, 50);   // Redis "lies": far too high (a bug, a bad restore, a crash...)

        List<Outcome<HoldView>> outcomes = everyoneBuysAtOnce(saleItemId, strategy);

        assertThat(successes(outcomes)).isEqualTo(1);
        assertThat(failures(outcomes)).allSatisfy(error ->
                assertThat(error).isInstanceOfAny(SoldOutException.class, StockBusyException.class));
        assertThat(unitsHeldOrSold(saleItemId)).isEqualTo(1);
        // All 50 got past Redis (50 -> 0). PostgreSQL kept 1 unit and the other 49 were given back: the counter still
        // lies, but no unit was oversold, lost or made up.
        assertThat(gateCounter(saleItemId)).isEqualTo(49);
    }

    @Test
    void whenRedisSaysMoreThanThereIsTheNaiveStrategyOversellsAgain() {
        long saleItemId = saleItemWithStock(1);
        setGateCounter(saleItemId, 50);

        List<Outcome<HoldView>> outcomes = everyoneBuysAtOnce(saleItemId, ConcurrencyStrategy.NAIVE);

        // Why the database check has to stay: the gate is only as good as its counter. With an honest counter even
        // NAIVE sold one unit (test 1); with a wrong one, all 50 reach NAIVE's race again.
        assertThat(successes(outcomes)).as("buyers who got the only unit").isGreaterThan(1);
        assertThat(unitsHeldOrSold(saleItemId)).isGreaterThan(1);
    }

    @Test
    void whenPostgresSaysNoTheUnitsGoBackToRedis() {
        long[] ids = saleItemsWithStock(5, 0);
        setGateCounter(ids[1], 5);   // Redis thinks item 2 has 5 left; PostgreSQL knows it has none

        // ALL_OR_NOTHING: Redis takes both lines, PostgreSQL finds item 2 short and rolls the whole cart back
        assertThatThrownBy(() -> holdService.createHold(cart("alice", ReservationMode.ALL_OR_NOTHING,
                line(ids[0], 1), line(ids[1], 1))))
                .isInstanceOf(SoldOutException.class)
                .hasMessageContaining("Sale item " + ids[1]);

        assertThat(gateCounter(ids[0])).as("item 1's unit came back to Redis").isEqualTo(5);
        assertThat(gateCounter(ids[1])).as("item 2's too (the counter is still wrong, but lost nothing)").isEqualTo(5);
        assertThat(availableStock(ids[0])).isEqualTo(5);
    }

    // ===================== Edge cases =====================

    @Test
    void aRetryOfTheRequestThatTookTheLastUnitGetsItsHoldBack() {
        long saleItemId = saleItemWithStock(1);
        CreateHoldCommand command = holdCommand("alice", "last-unit", saleItemId, 1);
        HoldView first = holdService.createHold(command).body();   // Redis: 1 -> 0

        // Alice never saw the answer and retries with the same key. Redis now says 0 left, but the replay check runs
        // before the gate, so she gets her hold, not SOLD_OUT.
        IdempotentResult<HoldView> retry = holdService.createHold(command);

        assertThat(retry.replayed()).isTrue();
        assertThat(retry.body().holdId()).isEqualTo(first.holdId());
        assertThat(gateCounter(saleItemId)).isZero();
    }

    @Test
    void aMissingCounterIsSeededFromPostgres() {
        long saleItemId = saleItemWithStock(5);
        assertThat(gateCounter(saleItemId)).as("no counter before the first buyer").isNull();

        holdFor("alice", saleItemId, 2);                          // seeds 5 (from PostgreSQL), takes 2
        assertThat(gateCounter(saleItemId)).isEqualTo(3);
        assertThat(redis.getExpire(gateKey(saleItemId), TimeUnit.SECONDS)).isBetween(1L, 60L);

        flushRedis();                                             // Redis restarted (or the counter expired)
        holdFor("bob", saleItemId, 1);                            // seeds 3 (from PostgreSQL), takes 1

        assertThat(gateCounter(saleItemId)).isEqualTo(2);
        assertThat(availableStock(saleItemId)).isEqualTo(2);
    }

    @Test
    void cancelAndExpiryGiveUnitsBackToRedis() {
        long saleItemId = saleItemWithStock(5);
        HoldView alice = holdFor("alice", saleItemId, 2);
        holdFor("bob", saleItemId, 1);
        assertThat(gateCounter(saleItemId)).isEqualTo(2);

        holdService.cancelHold("alice", alice.holdId());
        assertThat(gateCounter(saleItemId)).as("after alice cancels").isEqualTo(4);

        clock.advance(Duration.ofMinutes(11));
        expiryService.expireDueHolds();
        assertThat(gateCounter(saleItemId)).as("after bob's hold expires").isEqualTo(5);
        assertGateMatchesDatabase();
    }

    @Test
    void stockGivenBackInATransactionThatRollsBackNeverReachesRedis() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 2);                    // PostgreSQL 3, Redis 3
        HeldStock held = new HeldStock(hold.saleId(), List.of(line(saleItemId, 2)));

        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
            stockReleaser.release(List.of(held));                            // PostgreSQL +2, not committed yet
            throw new IllegalStateException("something fails before COMMIT");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(availableStock(saleItemId)).as("PostgreSQL rolled back").isEqualTo(3);
        assertThat(gateCounter(saleItemId)).as("so the after-commit hook never ran").isEqualTo(3);
    }

    @Test
    void redisAndPostgresAgreeAfterAMixOfEverything() {
        long[] ids = saleItemsWithStock(5, 3, 4);   // A, B, C

        HoldView alice = holdService.createHold(cart("alice", ReservationMode.ALL_OR_NOTHING,
                line(ids[0], 2), line(ids[1], 1))).body();                              // A 3, B 2
        HoldView bob = holdService.createHold(cart("bob", ReservationMode.PARTIAL,
                line(ids[1], 5), line(ids[2], 1))).body();                              // B short: C only (C 3)
        assertThat(bob.unavailable()).extracting(HoldView.UnavailableItem::saleItemId).containsExactly(ids[1]);
        assertThatThrownBy(() -> holdService.createHold(cart("carol", ReservationMode.ALL_OR_NOTHING,
                line(ids[0], 1), line(ids[1], 3)))).isInstanceOf(SoldOutException.class);  // refused by Redis
        HoldView dave = holdFor("dave", ids[0], 1);                                     // A 2
        holdService.cancelHold("dave", dave.holdId());                                 // A 3
        HoldView erin = holdFor("erin", ids[2], 2);                                     // C 1
        assertThat(pay("erin", erin.holdId(), "tok_success").holdStatus()).isEqualTo(HoldStatus.CONFIRMED);
        HoldView frank = holdFor("frank", ids[2], 1);                                   // C 0
        assertThat(pay("frank", frank.holdId(), "tok_declined").holdStatus()).isEqualTo(HoldStatus.HELD);

        clock.advance(Duration.ofMinutes(11));
        assertThat(expiryService.expireDueHolds()).isEqualTo(3);                      // alice, bob, frank

        assertThat(Arrays.stream(ids).mapToObj(this::gateCounter)).as("every item has a counter").doesNotContainNull();
        assertGateMatchesDatabase();
        assertThat(availableStock(ids[0])).isEqualTo(5);
        assertThat(availableStock(ids[1])).isEqualTo(3);
        assertThat(availableStock(ids[2])).as("erin's 2 units are sold").isEqualTo(2);
        assertThat(alice.totalAmount()).isEqualByComparingTo("400.00");                // 2 x 100.00 + 1 x 200.00
    }

    @Test
    void withTheGateOffRedisIsNeverAsked() {
        long saleItemId = saleItemWithStock(5);

        holdService.createHold(holdCommand("alice", "k1", saleItemId, 1).withStockGate(false));

        assertThat(gateCounter(saleItemId)).isNull();
        assertThat(availableStock(saleItemId)).isEqualTo(4);
    }

    // ===================== helpers =====================

    private List<Outcome<HoldView>> everyoneBuysAtOnce(long saleItemId, ConcurrencyStrategy strategy) {
        // The gate follows application.yml here: on.
        return Concurrently.run(BUYERS, buyer -> holdService.createHold(new CreateHoldCommand(
                "buyer-" + buyer, "key-" + buyer, strategy,
                List.of(new CreateHoldCommand.Item(saleItemId, 1)))).body());
    }

    /** How many requests the gate has counted with this outcome so far (passed, refused, off, redis_down). */
    private double gateCount(String outcome) {
        Counter counter = meters.find("flashsale.stock.gate").tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
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
