package com.flashsale.inventory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.flashsale.catalog.SaleItem;
import com.flashsale.catalog.SaleItemRepository;
import com.flashsale.common.Pause;
import com.flashsale.common.error.NotFoundException;

/**
 * No lock while working. Every write is "UPDATE ... WHERE id = ? AND version = ?". If another buyer committed
 * after we read the row, the version doesn't match, 0 rows change, and Hibernate throws. We then retry the
 * WHOLE cart with fresh data.
 * Good: nobody waits when clashes are rare. Bad in a flash sale: almost every attempt clashes, so buyers
 * burn retries (we measured it 2 to 3 times slower than PESSIMISTIC with 10 units and 50 buyers).
 *
 *               Hibernate sends them in primary-key order (hibernate.order_updates in application.yml), so two carts
 *               sharing items still can't deadlock.
 */
@Component
class OptimisticStockReserver implements StockReserver {

    private final SaleItemRepository saleItems;
    private final TransactionTemplate tx;
    private final InventoryProperties properties;

    OptimisticStockReserver(SaleItemRepository saleItems, TransactionTemplate tx, InventoryProperties properties) {
        this.saleItems = saleItems;
        this.tx = tx;
        this.properties = properties;
    }

    @Override
    public ConcurrencyStrategy strategy() {
        return ConcurrencyStrategy.OPTIMISTIC;
    }

    @Override
    public <T> T reserve(List<StockLine> lines, ReservationPolicy policy, Function<List<StockLine>, T> saveHold) {
        for (int attempt = 1; ; attempt++) {
            try {
                return tx.execute(status -> {
                    List<StockLine> taken = new ArrayList<>();
                    for (StockLine line : StockLine.inIdOrder(lines)) {
                        SaleItem item = saleItems.findById(line.saleItemId())
                                .orElseThrow(() -> new NotFoundException("Sale item", line.saleItemId()));
                        if (!item.hasAtLeast(line.quantity())) {
                            policy.onShortage(line); // ALL_OR_NOTHING throws; PARTIAL returns: skip this line
                            continue;
                        }
                        item.take(line.quantity());
                        taken.add(line);
                    }
                    return saveHold.apply(taken);
                });
            } catch (ObjectOptimisticLockingFailureException clash) {
                if (attempt >= properties.optimisticMaxAttempts()) {
                    throw new StockBusyException(lines); // gave up: HTTP 503, "please retry"
                }
                Pause.randomMillis(1, 10L * attempt); // back off a little, with randomness, so we don't collide again
            }
        }
    }
}
