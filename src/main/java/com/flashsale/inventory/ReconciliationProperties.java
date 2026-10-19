package com.flashsale.inventory;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;

import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The settings under "flashsale.reconciliation": whether the stock reconciliation job runs, and how often.
 */
@ConfigurationProperties("flashsale.reconciliation")
@Validated
public record ReconciliationProperties(
        boolean enabled,                                  // flashsale.reconciliation.enabled: false = no job (tests)
        @NotNull @DurationMin(millis = 1) Duration interval) {   // ...interval: pause between two runs
}
