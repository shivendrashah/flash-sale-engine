package com.flashsale.demo.web;

import jakarta.validation.Valid;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flashsale.demo.CrowdRequest;
import com.flashsale.demo.CrowdSimulator;
import com.flashsale.demo.SaleStats;
import com.flashsale.demo.SaleStatsService;

/**
 * The demo page's two endpoints:
 *   POST /api/admin/sales/{saleId}/crowd   sends fake buyers (CrowdSimulator). 202 Accepted: they run on.
 *   GET  /api/admin/sales/{saleId}/stats   the sale's live numbers (SaleStatsService)
 */
@RestController
@RequestMapping("/api/admin/sales/{saleId}")
@ConditionalOnProperty(prefix = "flashsale.demo", name = "enabled", havingValue = "true", matchIfMissing = true)
class DemoController {

    private final CrowdSimulator crowd;
    private final SaleStatsService stats;

    DemoController(CrowdSimulator crowd, SaleStatsService stats) {
        this.crowd = crowd;
        this.stats = stats;
    }

    @PostMapping("/crowd")
    ResponseEntity<SaleStats> sendCrowd(@PathVariable long saleId, @Valid @RequestBody CrowdRequest request) {
        crowd.send(saleId, request);
        return ResponseEntity.accepted().body(stats.stats(saleId));
    }

    @GetMapping("/stats")
    SaleStats stats(@PathVariable long saleId) {
        return stats.stats(saleId);
    }
}
