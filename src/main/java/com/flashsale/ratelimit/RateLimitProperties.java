package com.flashsale.ratelimit;

import jakarta.validation.constraints.Positive;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The settings under "flashsale.rate-limit": the size of each buyer's token bucket and how fast it refills.
 *   capacity: 10, refill-per-second: 5  ->  a burst of 10 requests at once, then 5 per second for as long as
 *   the buyer keeps going. A bucket left alone for 2 s (10 / 5) is full again.
 */
@ConfigurationProperties("flashsale.rate-limit")
@Validated
public record RateLimitProperties(
        boolean enabled,                       // flashsale.rate-limit.enabled: false = nothing is counted
        @Positive int capacity,                // flashsale.rate-limit.capacity: the most tokens a bucket holds
        @Positive double refillPerSecond) {    // flashsale.rate-limit.refill-per-second: tokens added each second
}
