package com.flashsale.hold;

import java.math.BigDecimal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

/**
 * One line of a hold: which sale item, how many, and the price at the moment of holding.
 * The price is copied ("snapshotted"), so a later price change doesn't change what the user pays.
 */
@Entity
@Table(name = "hold_items")
public class HoldItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "hold_id", nullable = false)
    private Hold hold;

    /** Refers to the SaleItem by id, not by object: SaleItem belongs to another aggregate. */
    @Column(name = "sale_item_id", nullable = false)
    private Long saleItemId;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    protected HoldItem() {
    }

    HoldItem(Hold hold, long saleItemId, int quantity, BigDecimal unitPrice) {
        this.hold = hold;
        this.saleItemId = saleItemId;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
    }

    BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }

    public Long getSaleItemId() {
        return saleItemId;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    /** Input for {@link Hold#create}: what to hold and at which price. */
    public record Line(long saleItemId, int quantity, BigDecimal unitPrice) {
    }
}
