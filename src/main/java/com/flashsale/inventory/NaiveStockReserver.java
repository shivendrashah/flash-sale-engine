package com.flashsale.inventory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.flashsale.catalog.SaleItemRepository;
import com.flashsale.common.Pause;
import com.flashsale.common.error.NotFoundException;

/**
 * BROKEN ON PURPOSE. Shows the classic "lost update" bug that causes overselling. OversellTest proves that
 * 50 buyers clicking at once can ALL get the last unit (we measured 20 winners for 1 unit).
 *
 * <pre>
 *   Buyer A reads stock = 1      Buyer B reads stock = 1      (both read before anyone writes)
 *   A: 1 >= 1, OK                B: 1 >= 1, OK
 *   A writes stock = 0           B writes stock = 0           (B overwrote A's write; both got the last unit)
 * </pre>
 * The stock column ends at 0, so it even LOOKS correct. Only the extra holds show the damage.
 * With a cart, each line has the same bug. One buyer on their own still gets the right answer, which is why the
 * single-buyer cart tests pass for NAIVE too: the bug only shows when buyers overlap.
 */
@Component
class NaiveStockReserver implements StockReserver {

    private final SaleItemRepository saleItems;
    private final TransactionTemplate tx;
    private final InventoryProperties properties;

    NaiveStockReserver(SaleItemRepository saleItems, TransactionTemplate tx, InventoryProperties properties) {
        this.saleItems = saleItems;
        this.tx = tx;
        this.properties = properties;
    }

    @Override
    public ConcurrencyStrategy strategy() {
        return ConcurrencyStrategy.NAIVE;
    }

    @Override
    public <T> T reserve(List<StockLine> lines, ReservationPolicy policy, Function<List<StockLine>, T> saveHold) {
        return tx.execute(status -> {
            List<StockLine> taken = new ArrayList<>();
            for (StockLine line : StockLine.inIdOrder(lines)) {
                int available = saleItems.findAvailableStock(line.saleItemId())
                        .orElseThrow(() -> new NotFoundException("Sale item", line.saleItemId()));
                if (available < line.quantity()) {
                    policy.onShortage(line); // ALL_OR_NOTHING throws here (ROLLBACK); PARTIAL returns: skip this line
                    continue;
                }
                // Real code often does work here (pricing, fraud checks). Meanwhile other buyers read the same old
                // number. Tests set naive-race-window=50ms so the race happens every time; the bug exists without it.
                Pause.of(properties.naiveRaceWindow());
                saleItems.overwriteAvailableStock(line.saleItemId(), available - line.quantity());
                taken.add(line);
            }
            return saveHold.apply(taken);
        });
    }
}
