package com.flashsale.inventory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs the stock reconciliation job every 10 seconds, in the background.
 *
 *       lifetime. Slower and the job would mostly arrive after the counter had healed itself (HLD.md 9.14).
 */
@Component
@ConditionalOnProperty(prefix = "flashsale.reconciliation", name = "enabled", havingValue = "true",
        matchIfMissing = true)
class StockReconciliationScheduler {

    private final StockReconciler reconciler;

    StockReconciliationScheduler(StockReconciler reconciler) {
        this.reconciler = reconciler;
    }

    @Scheduled(fixedDelayString = "${flashsale.reconciliation.interval}")
    void reconcile() {
        reconciler.reconcile();   // it logs what it finds itself
    }
}
