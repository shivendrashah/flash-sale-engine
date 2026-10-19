package com.flashsale.inventory;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The settings under "flashsale.inventory" in application.yml, as a typed Java record.
 */
@ConfigurationProperties("flashsale.inventory")
public record InventoryProperties(
        ConcurrencyStrategy defaultStrategy,  // flashsale.inventory.default-strategy
        int optimisticMaxAttempts,            // flashsale.inventory.optimistic-max-attempts
        Duration redisLockTtl,                // flashsale.inventory.redis-lock-ttl
        Duration redisLockWait,               // flashsale.inventory.redis-lock-wait
        Duration naiveRaceWindow,             // flashsale.inventory.naive-race-window
        boolean stockGateEnabled,             // flashsale.inventory.stock-gate-enabled
        Duration stockGateTtl) {              // flashsale.inventory.stock-gate-ttl
}
