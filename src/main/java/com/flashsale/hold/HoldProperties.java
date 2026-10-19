package com.flashsale.hold;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The settings under "flashsale.holds" in application.yml, as a typed record.
 */
@ConfigurationProperties("flashsale.holds")
public record HoldProperties(
        Duration ttl,                 // flashsale.holds.ttl: how long a hold keeps the stock
        boolean expiryJobEnabled,     // flashsale.holds.expiry-job-enabled
        Duration expiryJobInterval,   // flashsale.holds.expiry-job-interval
        int expiryBatchSize) {        // flashsale.holds.expiry-batch-size
}
