package com.flashsale.order.web;

import jakarta.validation.constraints.NotBlank;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flashsale.common.ApiHeaders;
import com.flashsale.order.OrderService;
import com.flashsale.order.OrderView;

/**
 * GET /api/orders/{orderId}: shows one order to its owner.
 */
@RestController
@RequestMapping("/api/orders")
class OrderController {

    private final OrderService orderService;

    OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/{orderId}")
    OrderView getOrder(@RequestHeader(ApiHeaders.USER_ID) @NotBlank String userId, @PathVariable long orderId) {
        return orderService.getOrder(userId, orderId);
    }
}
