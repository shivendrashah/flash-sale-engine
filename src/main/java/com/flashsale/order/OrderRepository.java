package com.flashsale.order;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Database access for orders.
 */
public interface OrderRepository extends JpaRepository<Order, Long> {

    Optional<Order> findByHoldId(long holdId);
}
