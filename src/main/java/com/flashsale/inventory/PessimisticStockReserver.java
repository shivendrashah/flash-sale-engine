package com.flashsale.inventory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.flashsale.catalog.SaleItem;
import com.flashsale.catalog.SaleItemRepository;
import com.flashsale.common.error.NotFoundException;

/**
 * Locks each stock row first. Buyers of the same item form a queue INSIDE PostgreSQL: each one reads the
 * stock only after the previous one has committed, so nobody ever sees an old number.
 * Good: simple and always correct. Cost: buyers wait, and each waiting buyer holds a DB connection.
 *
 * The SQL this sends for a one-item cart, in order (copied from the logs with the "learn" profile):
 *   1. select ... from sale_items where id=? for no key update   <- the lock; other buyers wait HERE
 *   2. insert into holds (...) values (...)                       <- from saveHold.apply(taken)
 *   3. insert into hold_items (...) values (...)
 *   4. update sale_items set available_stock=?, ..., version=? where id=? and version=?   <- dirty checking
 *   5. COMMIT                                                     <- lock released; the next buyer goes
 * A two-item cart (sale items 4 and 5, from the "learn" logs) shows Hibernate's "auto-flush": before it runs the
 * second SELECT on sale_items, it sends the pending UPDATE for item 4, so the query can't miss our own change:
 *   select ... where id=4 for no key update      <- lock 1 (lowest id first)
 *   update sale_items ... where id=4 and version=?    <- auto-flush, under lock 1
 *   select ... where id=5 for no key update      <- lock 2
 *   insert into holds; insert into hold_items (x2); update sale_items ... where id=5 ...; COMMIT
 */
@Component
class PessimisticStockReserver implements StockReserver {

    private final SaleItemRepository saleItems;
    private final TransactionTemplate tx;

    PessimisticStockReserver(SaleItemRepository saleItems, TransactionTemplate tx) {
        this.saleItems = saleItems;
        this.tx = tx;
    }

    @Override
    public ConcurrencyStrategy strategy() {
        return ConcurrencyStrategy.PESSIMISTIC;
    }

    @Override
    public <T> T reserve(List<StockLine> lines, ReservationPolicy policy, Function<List<StockLine>, T> saveHold) {
        return tx.execute(status -> {
            List<StockLine> taken = new ArrayList<>();
            for (StockLine line : StockLine.inIdOrder(lines)) {
                SaleItem item = saleItems.findByIdForUpdate(line.saleItemId())
                        .orElseThrow(() -> new NotFoundException("Sale item", line.saleItemId()));
                if (!item.hasAtLeast(line.quantity())) {
                    policy.onShortage(line); // ALL_OR_NOTHING throws (ROLLBACK); PARTIAL returns: skip this line
                    continue;                // (a skipped row stays locked until COMMIT; harmless and short)
                }
                item.take(line.quantity());
                taken.add(line);
            }
            return saveHold.apply(taken);
        });
    }
}
