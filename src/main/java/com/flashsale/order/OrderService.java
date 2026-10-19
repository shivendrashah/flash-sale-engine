package com.flashsale.order;

import org.springframework.stereotype.Service;

import com.flashsale.common.error.NotFoundException;

/**
 * Reads an order for its owner. Someone else's order looks "not found" (404).
 */
@Service
public class OrderService {

    private final OrderRepository orders;

    public OrderService(OrderRepository orders) {
        this.orders = orders;
    }

    public OrderView getOrder(String userId, long orderId) {
        return orders.findById(orderId)
                .filter(order -> order.getUserId().equals(userId))
                .map(OrderView::of)
                .orElseThrow(() -> new NotFoundException("Order", orderId));
    }
}
