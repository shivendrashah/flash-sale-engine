package com.flashsale.catalog;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * One product inside one sale, with limited stock. availableStock is how many units can still be held.
 * THIS is the row thousands of buyers try to change at the same moment (the "hot row").
 */
@Entity
@Table(name = "sale_items")
public class SaleItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "sale_id", nullable = false)
    private Long saleId;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "sale_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal salePrice;

    @Column(name = "total_stock", nullable = false)
    private int totalStock;

    @Column(name = "available_stock", nullable = false)
    private int availableStock;

    @Version
    @Column(nullable = false)
    private long version;

    protected SaleItem() {
    }

    public SaleItem(Long saleId, Long productId, BigDecimal salePrice, int stock) {
        this.saleId = saleId;
        this.productId = productId;
        this.salePrice = salePrice;
        this.totalStock = stock;
        this.availableStock = stock;
    }

    /**
     * Takes units out of stock.
     */
    public void take(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive, got " + quantity);
        }
        if (availableStock < quantity) {
            throw new SoldOutException(id, quantity); // becomes HTTP 409 SOLD_OUT
        }
        availableStock -= quantity;
    }

    /**
     * True if {@code quantity} units can be taken right now. The stock strategies ask this first, so a short cart
     * item goes to the ReservationPolicy (skip it, or give up on the whole cart) instead of always throwing.
     */
    public boolean hasAtLeast(int quantity) {
        return availableStock >= quantity;
    }

    public Long getId() {
        return id;
    }

    public Long getSaleId() {
        return saleId;
    }

    public Long getProductId() {
        return productId;
    }

    public BigDecimal getSalePrice() {
        return salePrice;
    }

    public int getTotalStock() {
        return totalStock;
    }

    public int getAvailableStock() {
        return availableStock;
    }
}
