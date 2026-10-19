package com.flashsale.catalog;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Database access for products.
 */
public interface ProductRepository extends JpaRepository<Product, Long> {
}
