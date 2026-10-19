package com.flashsale.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.hold.HoldView;
import com.flashsale.support.IntegrationTest;

/**
 * The stock reconciliation job:
 *   a counter that agrees with PostgreSQL is left alone, and a missing one is not created;
 *   a counter left low (as a crash would leave it) is reported on the first run and fixed on the second;
 *   a difference that moves between runs (a sale in progress) is not fixed until it stands still;
 *   the fix never overwrites a counter a buyer changed after the job read it (compare-and-set);
 *   ended sales are not checked.
 */
class StockReconcilerTest extends IntegrationTest {

    @Autowired
    StockReconciler reconciler;

    @Autowired
    MeterRegistry meters;

    @BeforeEach
    void theJobHasSeenNothingYet() {
        reconciler.forgetLastRun();
    }

    @Test
    void aCounterThatAgreesIsLeftAloneAndAMissingOneIsNotCreated() {
        long bought = saleItemWithStock(5);
        long untouched = saleItemWithStock(5);       // nobody bought it: no counter in Redis
        holdFor("alice", bought, 1);                 // counter seeded and taken: Redis 4, PostgreSQL 4

        StockReconciler.Result result = reconciler.reconcile();

        assertThat(result).isEqualTo(new StockReconciler.Result(1, 0, 0));
        assertThat(gateCounter(bought)).isEqualTo(4);
        assertThat(gateCounter(untouched)).as("not seeded by the job").isNull();
        assertThat(drift()).isZero();
    }

    @Test
    void aCounterLeftLowIsReportedThenFixedOnTheNextRun() {
        long saleItemId = saleItemWithStock(5);
        holdFor("alice", saleItemId, 1);             // Redis 4, PostgreSQL 4
        setGateCounter(saleItemId, 1);               // as if 3 units' give-back had been lost: "1 left", truly 4
        redis.expire(gateKey(saleItemId), Duration.ofSeconds(60));   // a plain SET drops the expiry: put it back

        StockReconciler.Result first = reconciler.reconcile();
        assertThat(first).isEqualTo(new StockReconciler.Result(1, 1, 0));
        assertThat(gateCounter(saleItemId)).as("not fixed on first sight").isEqualTo(1);
        assertThat(drift()).as("units of drift").isEqualTo(3);

        StockReconciler.Result second = reconciler.reconcile();
        assertThat(second).isEqualTo(new StockReconciler.Result(1, 1, 1));
        assertThat(gateCounter(saleItemId)).as("now PostgreSQL's number").isEqualTo(4);
        assertThat(redis.getExpire(gateKey(saleItemId), TimeUnit.MILLISECONDS)).isPositive();

        assertThat(reconciler.reconcile()).isEqualTo(new StockReconciler.Result(1, 0, 0));
        assertThat(drift()).isZero();
        assertGateMatchesDatabase();
    }

    @Test
    void aDifferenceThatMovesBetweenRunsIsNotFixed() {
        long saleItemId = saleItemWithStock(10);
        holdFor("alice", saleItemId, 1);             // Redis 9, PostgreSQL 9
        setGateCounter(saleItemId, 7);
        reconciler.reconcile();                      // sees (7, 9)

        setGateCounter(saleItemId, 6);               // the counter moved: something is going on (a sale in flight)
        StockReconciler.Result second = reconciler.reconcile();     // sees (6, 9): not the same as last time
        assertThat(second.fixed()).isZero();
        assertThat(gateCounter(saleItemId)).isEqualTo(6);

        assertThat(reconciler.reconcile().fixed()).as("stood still for a whole run: now fixed").isEqualTo(1);
        assertThat(gateCounter(saleItemId)).isEqualTo(9);
    }

    @Test
    void aCounterThatIsTooHighIsReportedButLeftForItsExpiry() {
        long saleItemId = saleItemWithStock(5);
        holdFor("alice", saleItemId, 1);             // Redis 4, PostgreSQL 4
        setGateCounter(saleItemId, 6);               // 2 too many: harmless, PostgreSQL still decides every sale

        reconciler.reconcile();
        StockReconciler.Result second = reconciler.reconcile();

        assertThat(second).isEqualTo(new StockReconciler.Result(1, 1, 0));
        assertThat(gateCounter(saleItemId)).as("not lowered: lowering is the risky direction").isEqualTo(6);
        assertThat(drift()).isEqualTo(2);
    }

    @Test
    void theFixNeverOverwritesACounterThatChangedAfterTheJobReadIt() {
        long saleItemId = saleItemWithStock(10);
        holdFor("alice", saleItemId, 1);             // Redis 9, PostgreSQL 9
        String key = gateKey(saleItemId);
        setGateCounter(saleItemId, 5);

        // The job saw 5, but a buyer takes a unit before the fix lands: the counter is 4 now
        setGateCounter(saleItemId, 4);
        boolean fixed = reconciler.fix(key, new StockReconciler.Observation(5, 9));

        assertThat(fixed).isFalse();
        assertThat(gateCounter(saleItemId)).as("the buyer's change survives").isEqualTo(4);
        assertThat(reconciler.fix(key, new StockReconciler.Observation(4, 9))).isTrue();
        assertThat(gateCounter(saleItemId)).isEqualTo(9);
    }

    @Test
    void endedSalesAreNotChecked() {
        long saleItemId = saleItemWithStock(5);
        holdFor("alice", saleItemId, 1);
        setGateCounter(saleItemId, 0);
        clock.advance(Duration.ofHours(2));          // the test sale ran for an hour: it is over

        assertThat(reconciler.reconcile().checked()).isZero();
        assertThat(gateCounter(saleItemId)).as("left alone; it expires by itself").isZero();
    }

    @Test
    void theRealCaseAGiveBackLostWhileRedisWasAwayIsRepaired() {
        long saleItemId = saleItemWithStock(3);
        HoldView hold = holdFor("alice", saleItemId, 1);   // Redis 2, PostgreSQL 2
        // A cancel puts the unit back in PostgreSQL (3). Its give-back to Redis runs after the commit; if Redis is
        // unreachable at that moment, it is lost and the counter stays at 2 (RedisOutageTest shows that failure).
        // Here Redis is up, so the lost give-back is played by putting the counter back to 2 afterwards.
        holdService.cancelHold("alice", hold.holdId());
        setGateCounter(saleItemId, 2);
        assertThat(availableStock(saleItemId)).isEqualTo(3);

        reconciler.reconcile();
        reconciler.reconcile();

        assertThat(gateCounter(saleItemId)).as("the third unit is on sale again in Redis").isEqualTo(3);
        assertThat(meters.counter("flashsale.stock.reconcile", "outcome", "fixed").count()).isPositive();
    }

    @Test
    void whileBuyersBuyAndCancelTheJobNeverLeavesTheCounterLow() throws Exception {
        long saleItemId = saleItemWithStock(200);
        holdFor("seed", saleItemId, 1);              // the counter exists before the crowd arrives
        java.util.concurrent.atomic.AtomicBoolean buying = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
        Thread job = new Thread(() -> {
            while (buying.get()) {                   // the job, as fast as it can, the whole time
                reconciler.reconcile();
                runs.incrementAndGet();
            }
        });
        job.start();

        var buyers = com.flashsale.support.Concurrently.run(60, i -> {
            HoldView hold = holdFor("buyer-" + i, saleItemId, 1);
            if (i % 2 == 0) {
                holdService.cancelHold("buyer-" + i, hold.holdId());   // the unit goes back: PostgreSQL, then Redis
            }
            return hold;
        });

        buying.set(false);
        job.join();
        assertThat(com.flashsale.support.Concurrently.failures(buyers)).as("nobody was turned away").isEmpty();
        assertThat(runs.get()).as("job runs during the sale").isGreaterThan(5);
        int available = availableStock(saleItemId);
        assertThat(available).isEqualTo(200 - 1 - 30);
        // never LOW; it may be a little HIGH after a mistaken fix, which the next buyer's check in PostgreSQL absorbs
        assertThat(gateCounter(saleItemId)).as("Redis counter vs PostgreSQL %d", available)
                .isGreaterThanOrEqualTo(available);
    }

    private double drift() {
        return meters.get("flashsale.stock.drift").gauge().value();
    }
}
