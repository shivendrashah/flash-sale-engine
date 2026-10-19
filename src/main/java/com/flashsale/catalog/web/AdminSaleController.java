package com.flashsale.catalog.web;

import java.net.URI;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flashsale.catalog.CreateSaleRequest;
import com.flashsale.catalog.SaleService;
import com.flashsale.catalog.SaleView;

/**
 * POST /api/admin/sales: creates a sale with its products and stock.
 * Phase 1 has no login; a real system would only let admins call this.
 */
@RestController
@RequestMapping("/api/admin/sales")
class AdminSaleController {

    private final SaleService saleService;

    AdminSaleController(SaleService saleService) {
        this.saleService = saleService;
    }

    @PostMapping
    ResponseEntity<SaleView> createSale(@Valid @RequestBody CreateSaleRequest request) {
        SaleView sale = saleService.createSale(request);
        return ResponseEntity.created(URI.create("/api/sales/" + sale.id())).body(sale);
    }
}
