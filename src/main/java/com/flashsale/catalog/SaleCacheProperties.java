package com.flashsale.catalog;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;

import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The settings under "flashsale.sale-cache": whether the sale page is cached in Redis, and for how long.
 *   ttl: 1s  ->  "units left" on the page can be up to about a second old. Each app instance then reads one
 *   sale from PostgreSQL about once a second, however many people are looking at it.
 */
@ConfigurationProperties("flashsale.sale-cache")
@Validated
public record SaleCacheProperties(
        boolean enabled,              // flashsale.sale-cache.enabled: false = every page read goes to PostgreSQL
        // flashsale.sale-cache.ttl: how long one copy of a sale page lives in Redis
        @NotNull @DurationMin(millis = 1) Duration ttl) {
}
