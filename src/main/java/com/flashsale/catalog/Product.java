package com.flashsale.catalog;

import java.math.BigDecimal;
import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Something we sell, e.g. "iPhone 17". It exists on its own; a SaleItem puts it into a sale with a sale
 * price and limited stock (the same iPhone can be in many sales).
 */
@Entity
@Table(name = "products")
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 200)
    private String name;

    @Column(name = "regular_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal regularPrice;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Product() {
    }

    public Product(String name, BigDecimal regularPrice, Instant createdAt) {
        this.name = name;
        this.regularPrice = regularPrice;
        this.createdAt = createdAt;
    }

    // Getters only. There are no setters: this data doesn't change after creation.

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getRegularPrice() {
        return regularPrice;
    }
}
