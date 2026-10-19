package com.flashsale.hold;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.flashsale.support.Concurrently;
import com.flashsale.support.Concurrently.Outcome;
import com.flashsale.support.IntegrationTest;

/**
 * Tests the expiry job. The scheduler is switched off in tests, so each test moves the clock
 * forward (clock.advance) and runs the job by hand at exactly the moment it wants.
 */
class HoldExpiryTest extends IntegrationTest {

    @Autowired
    HoldExpiryService expiryService;

    @Test
    void expiredHoldsPutTheirStockBackOnSale() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 2);
        assertThat(availableStock(saleItemId)).isEqualTo(3);

        clock.advance(Duration.ofMinutes(10).plusSeconds(1)); // 10 minutes and 1 second later, instantly
        int expired = expiryService.expireDueHolds();

        assertThat(expired).isEqualTo(1);
        assertThat(holdService.getHold("alice", hold.holdId()).status()).isEqualTo(HoldStatus.EXPIRED);
        assertThat(availableStock(saleItemId)).isEqualTo(5);
    }

    @Test
    void holdsAreKeptUntilTheirTimeIsUp() {
        long saleItemId = saleItemWithStock(5);
        holdFor("alice", saleItemId, 2);

        clock.advance(Duration.ofMinutes(9));

        assertThat(expiryService.expireDueHolds()).isZero();
        assertThat(availableStock(saleItemId)).isEqualTo(3);
    }

    @Test
    void anExpiredHoldCannotBePaidEvenBeforeTheJobRuns() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 1);

        clock.advance(Duration.ofMinutes(11));

        assertThatThrownBy(() -> pay("alice", hold.holdId(), "tok_success"))
                .isInstanceOf(HoldExpiredException.class);
    }

    @Test
    void holdsInTheMiddleOfPayingAreNotExpired() {
        long saleItemId = saleItemWithStock(5);
        HoldView hold = holdFor("alice", saleItemId, 1);
        // Simulate a payment that is still waiting on the gateway (plain SQL, bypassing the app).
        jdbc.update("UPDATE holds SET status = 'PAYMENT_PENDING' WHERE id = ?", hold.holdId());

        clock.advance(Duration.ofMinutes(30));

        assertThat(expiryService.expireDueHolds()).isZero();
        assertThat(availableStock(saleItemId)).isEqualTo(4);
    }

    @Test
    void twoExpiryJobsRunningAtOnceGiveEachUnitBackExactlyOnce() {
        int holdsToExpire = 250;
        long saleItemId = saleItemWithStock(holdsToExpire);
        for (int i = 0; i < holdsToExpire; i++) {
            holdFor("user-" + i, saleItemId, 1);
        }
        assertThat(availableStock(saleItemId)).isZero();

        clock.advance(Duration.ofMinutes(11));
        // Like two app instances whose schedulers fire at the same moment. SKIP LOCKED splits the work.
        List<Outcome<Integer>> runs = Concurrently.run(2, instance -> expiryService.expireDueHolds());

        assertThat(runs).allMatch(Outcome::succeeded);
        assertThat(runs.stream().mapToInt(Outcome::value).sum()).isEqualTo(holdsToExpire);
        assertThat(availableStock(saleItemId)).isEqualTo(holdsToExpire);
    }
}
