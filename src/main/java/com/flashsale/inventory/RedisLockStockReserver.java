package com.flashsale.inventory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.springframework.dao.DataAccessException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.flashsale.catalog.SaleItem;
import com.flashsale.catalog.SaleItemRepository;
import com.flashsale.common.error.NotFoundException;

/**
 * Takes a Redis lock per cart item, THEN does a normal read-check-write in PostgreSQL.
 * Waiting happens in Redis, not in the database, so queued buyers don't hold DB connections.
 *
 * Two rules make it safe:
 *   1. Unlock only AFTER the transaction commits. Unlocking earlier lets the next buyer read the old stock.
 *   2. Keep the database version check as a backstop. If our lock expired early (a long GC pause, say) and someone
 *      else got in, the version won't match and we fail instead of overselling. The version acts as a
 *      "fencing token".
 */
@Component
class RedisLockStockReserver implements StockReserver {

    private final RedisLock redisLock;
    private final SaleItemRepository saleItems;
    private final TransactionTemplate tx;
    private final InventoryProperties properties;

    RedisLockStockReserver(RedisLock redisLock, SaleItemRepository saleItems, TransactionTemplate tx,
                           InventoryProperties properties) {
        this.redisLock = redisLock;
        this.saleItems = saleItems;
        this.tx = tx;
        this.properties = properties;
    }

    @Override
    public ConcurrencyStrategy strategy() {
        return ConcurrencyStrategy.REDIS_LOCK;
    }

    @Override
    public <T> T reserve(List<StockLine> lines, ReservationPolicy policy, Function<List<StockLine>, T> saveHold) {
        List<StockLine> ordered = StockLine.inIdOrder(lines);
        List<RedisLock.Lease> leases = acquireAllOrFailClosed(ordered);   // step 1 (Redis)
        try {
            return tx.execute(status -> {                                 // step 2 (PostgreSQL)
                List<StockLine> taken = new ArrayList<>();
                for (StockLine line : ordered) {
                    SaleItem item = saleItems.findById(line.saleItemId())
                            .orElseThrow(() -> new NotFoundException("Sale item", line.saleItemId()));
                    if (!item.hasAtLeast(line.quantity())) {
                        policy.onShortage(line); // ALL_OR_NOTHING throws (ROLLBACK); PARTIAL returns: skip this line
                        continue;
                    }
                    item.take(line.quantity());
                    taken.add(line);
                }
                return saveHold.apply(taken);
            });
        } catch (ObjectOptimisticLockingFailureException lockExpiredWhileWorking) {
            throw new StockBusyException(ordered);
        } finally {
            releaseAll(leases);                                         // step 3 (Redis), AFTER commit
        }
    }

    /**
     * One lock per item, lowest id first, all within ONE shared wait budget (flashsale.inventory.redis-lock-wait).
     * No lock, no sale: if a lock is taken for too long or Redis is unreachable, tell the client to retry (503).
     */
    private List<RedisLock.Lease> acquireAllOrFailClosed(List<StockLine> ordered) {
        List<RedisLock.Lease> leases = new ArrayList<>();
        long deadline = System.nanoTime() + properties.redisLockWait().toNanos();
        try {
            for (StockLine line : ordered) {
                Duration timeLeft = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
                leases.add(redisLock
                        .tryAcquire("lock:sale-item:" + line.saleItemId(), properties.redisLockTtl(), timeLeft)
                        .orElseThrow(() -> new StockBusyException(line.saleItemId())));
            }
            return leases;
        } catch (StockBusyException busy) {
            releaseAll(leases);
            throw busy;
        } catch (DataAccessException redisDown) {
            releaseAll(leases); // best effort; if Redis is really down, the locks expire by their TTL anyway
            throw new StockBusyException(ordered);
        }
    }

    private void releaseAll(List<RedisLock.Lease> leases) {
        leases.forEach(redisLock::release);
    }
}
