package com.flashsale.inventory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.flashsale.catalog.SaleItemRepository;

/**
 * Gives stock back when a hold ends without a purchase (it expired or was cancelled): to PostgreSQL inside the
 * caller's transaction, then to the Redis stock gate once that transaction has committed.
 */
@Component
public class StockReleaser {

    private final SaleItemRepository saleItems;
    private final StockGate stockGate;

    public StockReleaser(SaleItemRepository saleItems, StockGate stockGate) {
        this.saleItems = saleItems;
        this.stockGate = stockGate;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void release(Collection<HeldStock> released) {
        if (released.isEmpty()) {
            return;
        }
        Map<Long, Integer> quantityByItem = new TreeMap<>();
        for (HeldStock stock : released) {
            for (StockLine line : stock.lines()) {
                quantityByItem.merge(line.saleItemId(), line.quantity(), Integer::sum);
            }
        }
        quantityByItem.forEach(saleItems::giveBack);

        List<HeldStock> toGiveBack = List.copyOf(released);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                stockGate.giveBack(toGiveBack); // best effort: if Redis is down, the counters stay low until they expire
            }
        });
    }
}
