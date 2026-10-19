package com.flashsale.catalog.web;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flashsale.catalog.SaleCache;
import com.flashsale.catalog.SaleService;
import com.flashsale.catalog.SaleView;

/**
 * GET /api/sales/{saleId}: shows a sale and how much stock is left.
 * GET /api/sales: the 20 newest sales (the shop's front page).
 */
@RestController
@RequestMapping("/api/sales")
class SaleController {

    private final SaleCache saleCache;
    private final SaleService saleService;

    SaleController(SaleCache saleCache, SaleService saleService) {
        this.saleCache = saleCache;
        this.saleService = saleService;
    }

    @GetMapping("/{saleId}")
    SaleView getSale(@PathVariable long saleId) {
        return saleCache.getSale(saleId); // returned object -> JSON body, status 200 OK
    }

    @GetMapping
    List<SaleView> listSales() {
        return saleService.listSales();
    }
}
