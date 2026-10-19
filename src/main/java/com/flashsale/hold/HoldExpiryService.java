package com.flashsale.hold;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import com.flashsale.inventory.HeldStock;
import com.flashsale.inventory.StockReleaser;

/**
 * Finds holds whose time is up, marks them EXPIRED, and puts their stock back on sale (in PostgreSQL, and
 * after the commit in the Redis stock gate). Their units stop counting against each buyer's purchase limit.
 */
@Service
public class HoldExpiryService {

    private final HoldRepository holds;
    private final StockReleaser stockReleaser;
    private final PurchaseLimits purchaseLimits;
    private final TransactionTemplate tx;
    private final HoldProperties properties;
    private final Clock clock;

    public HoldExpiryService(HoldRepository holds, StockReleaser stockReleaser, PurchaseLimits purchaseLimits,
                             TransactionTemplate tx, HoldProperties properties, Clock clock) {
        this.holds = holds;
        this.stockReleaser = stockReleaser;
        this.purchaseLimits = purchaseLimits;
        this.tx = tx;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * and a crash only rolls back one batch.
     *
     * @return how many holds this call expired
     */
    public int expireDueHolds() {
        int total = 0;
        while (true) {
            Integer expired = tx.execute(status -> expireOneBatch());
            int count = expired == null ? 0 : expired;
            total += count;
            if (count < properties.expiryBatchSize()) {
                return total; // a batch smaller than the limit means nothing more is due right now
            }
        }
    }

    private int expireOneBatch() {
        Instant now = clock.instant();
        List<Hold> due = holds.lockHoldsDueForExpiry(now, properties.expiryBatchSize());
        List<HeldStock> stockToGiveBack = new ArrayList<>();
        for (Hold hold : due) {
            hold.expire(now);
            stockToGiveBack.add(hold.heldStock());
        }
        stockReleaser.release(stockToGiveBack);
        purchaseLimits.giveBack(due);
        return due.size();
                                                          //   after it, the units go back to the Redis gate too
    }
}
