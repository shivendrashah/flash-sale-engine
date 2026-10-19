package com.flashsale.inventory;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * Picks the right StockReserver for a ConcurrencyStrategy.
 */
@Component
public class StockReserverFactory {

    private final Map<ConcurrencyStrategy, StockReserver> byStrategy = new EnumMap<>(ConcurrencyStrategy.class);
    private final ConcurrencyStrategy defaultStrategy;

    public StockReserverFactory(List<StockReserver> reservers, InventoryProperties properties) {
        for (StockReserver reserver : reservers) {
            if (byStrategy.put(reserver.strategy(), reserver) != null) {
                // Fail at startup, not at 10:00 AM during the sale.
                throw new IllegalStateException("Two StockReservers registered for " + reserver.strategy());
            }
        }
        this.defaultStrategy = properties.defaultStrategy(); // flashsale.inventory.default-strategy
    }

    /** @param requested the strategy the request asked for (X-Concurrency-Strategy header), or null for the default */
    public StockReserver get(ConcurrencyStrategy requested) {
        ConcurrencyStrategy strategy = requested != null ? requested : defaultStrategy;
        StockReserver reserver = byStrategy.get(strategy);
        if (reserver == null) {
            throw new IllegalStateException("No StockReserver registered for " + strategy);
        }
        return reserver;
    }
}
