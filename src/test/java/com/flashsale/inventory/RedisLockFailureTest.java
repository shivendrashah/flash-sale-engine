package com.flashsale.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.net.SocketException;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.transaction.support.TransactionTemplate;

import com.flashsale.catalog.SaleItemRepository;

/**
 * What happens when Redis is unreachable.
 */
class RedisLockFailureTest {

    @Test
    void ifRedisIsDownWhileLockingNothingIsSoldAndTheClientIsToldToRetry() {
        // Given: fakes for every dependency, and a RedisLock that fails like an unreachable Redis
        RedisLock redisLock = mock(RedisLock.class);
        SaleItemRepository saleItems = mock(SaleItemRepository.class);
        TransactionTemplate tx = mock(TransactionTemplate.class);
        when(redisLock.tryAcquire(anyString(), any(), any()))
                .thenThrow(new RedisConnectionFailureException("Redis is down"));
        RedisLockStockReserver reserver = new RedisLockStockReserver(redisLock, saleItems, tx,
                new InventoryProperties(ConcurrencyStrategy.REDIS_LOCK, 10, Duration.ofSeconds(5),
                        Duration.ofSeconds(3), Duration.ZERO, true, Duration.ofSeconds(60)));

        // When / Then: the buyer is told to retry (503)...
        assertThatThrownBy(() -> reserver.reserve(List.of(new StockLine(7L, 1)), new AllOrNothingPolicy(),
                taken -> "hold"))
                .isInstanceOf(StockBusyException.class);
        // ...and the database was never touched: no lock, no sale ("fail closed").
        verifyNoInteractions(tx, saleItems);
    }

    @Test
    void ifTheConnectionBreaksWhileLockingTheClientIsStillToldToRetry() {
        RedisLock redisLock = mock(RedisLock.class);
        SaleItemRepository saleItems = mock(SaleItemRepository.class);
        TransactionTemplate tx = mock(TransactionTemplate.class);
        when(redisLock.tryAcquire(anyString(), any(), any()))
                .thenThrow(new RedisSystemException("Redis exception", new SocketException("Connection reset")));
        RedisLockStockReserver reserver = new RedisLockStockReserver(redisLock, saleItems, tx,
                new InventoryProperties(ConcurrencyStrategy.REDIS_LOCK, 10, Duration.ofSeconds(5),
                        Duration.ofSeconds(3), Duration.ZERO, true, Duration.ofSeconds(60)));

        assertThatThrownBy(() -> reserver.reserve(List.of(new StockLine(7L, 1)), new AllOrNothingPolicy(),
                taken -> "hold"))
                .isInstanceOf(StockBusyException.class);
        verifyNoInteractions(tx, saleItems);
    }

    @Test
    void ifRedisIsDownWhileUnlockingTheLockIsLeftToExpire() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.execute(ArgumentMatchers.<RedisScript<Long>>any(), anyList(), any()))
                .thenThrow(new RedisConnectionFailureException("Redis is down"));

        boolean released = new RedisLock(redis).release(new RedisLock.Lease("lock:sale-item:7", "token"));

        assertThat(released).isFalse(); // no exception: the purchase that already committed still succeeds
    }
}
