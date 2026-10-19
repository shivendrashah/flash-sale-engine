package com.flashsale.catalog;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * What the API shows for a sale: its window, whether it's live, and how much of each item is left.
 *
 *       (lazy loading, internal fields like version). A view says exactly what the API promises.
 */
public record SaleView(long id, String name, Instant startsAt, Instant endsAt, boolean active,
                       @JsonInclude(JsonInclude.Include.NON_NULL) Integer perUserLimit,
                       @JsonInclude(JsonInclude.Include.NON_NULL) Integer admitPerSecond,
                       List<Item> items) {

    /**
     * This view with "active" worked out for the given moment. SaleCache stores a view for a second; the stock in it
     * may be a second old, but the sale's start and end never change, so "active" can always be exact.
     */
    public SaleView at(Instant now) {
        return new SaleView(id, name, startsAt, endsAt, Sale.isActiveAt(startsAt, endsAt, now), perUserLimit,
                admitPerSecond, items);
    }

    public record Item(long saleItemId, long productId, String productName, BigDecimal regularPrice,
                       BigDecimal salePrice, int totalStock, int availableStock) {
    }
}
