package com.flashsale.inventory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.flashsale.catalog.SaleItemRepository;
import com.flashsale.common.error.NotFoundException;

/**
 * Lets the database do the check AND the subtraction in one atomic statement, once per cart line:
 *   update sale_items set available_stock = available_stock - ? where id = ? and available_stock >= ?
 * "1 row updated" means we got the stock; "0 rows" means there wasn't enough.
 * Good: one round trip per item, no retries, no gap between check and write. Usually the best choice for a
 * counter.
 *
 * Notice the order differs from PESSIMISTIC: here each UPDATE runs at once (a @Modifying query runs immediately),
 * then the INSERTs from saveHold. In PESSIMISTIC the UPDATEs come last, when Hibernate flushes.
 */
@Component
class ConditionalUpdateStockReserver implements StockReserver {

    private final SaleItemRepository saleItems;
    private final TransactionTemplate tx;

    ConditionalUpdateStockReserver(SaleItemRepository saleItems, TransactionTemplate tx) {
        this.saleItems = saleItems;
        this.tx = tx;
    }

    @Override
    public ConcurrencyStrategy strategy() {
        return ConcurrencyStrategy.CONDITIONAL_UPDATE;
    }

    @Override
    public <T> T reserve(List<StockLine> lines, ReservationPolicy policy, Function<List<StockLine>, T> saveHold) {
        return tx.execute(status -> {
            List<StockLine> taken = new ArrayList<>();
            for (StockLine line : StockLine.inIdOrder(lines)) {
                int updated = saleItems.takeIfAvailable(line.saleItemId(), line.quantity());
                if (updated == 1) {
                    taken.add(line); // the UPDATE also locked the row: it stays ours until COMMIT, like FOR UPDATE
                    continue;
                }
                if (!saleItems.existsById(line.saleItemId())) {
                    throw new NotFoundException("Sale item", line.saleItemId());
                }
                policy.onShortage(line);
            }
            return saveHold.apply(taken);
        });
    }
}
