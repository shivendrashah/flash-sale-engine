package com.flashsale.inventory;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record StockLine(long saleItemId, int quantity) {

    /**
     * The lines sorted by sale item id: the ONE order in which every strategy visits (and so locks) stock rows.
     * Fails on a repeated id: one line per item is a rule the API already enforces (CreateHoldRequest).
     */
    public static List<StockLine> inIdOrder(Collection<StockLine> lines) {
        Set<Long> seen = new HashSet<>();
        for (StockLine line : lines) {
            if (!seen.add(line.saleItemId())) {
                throw new IllegalArgumentException("Sale item " + line.saleItemId() + " is listed twice");
            }
        }
        return lines.stream().sorted(Comparator.comparingLong(StockLine::saleItemId)).toList();
    }
}
