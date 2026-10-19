package com.flashsale.demo;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;

import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The settings under "flashsale.demo": whether the crowd simulator exists at all, and how its fake buyers
 * behave.
 */
@ConfigurationProperties("flashsale.demo")
@Validated
public record DemoProperties(
        boolean enabled,                                         // false = no crowd endpoint (a real deployment)
        @NotNull Duration thinkTime,                             // a fake buyer waits up to this long before paying
        @NotNull @DurationMin(millis = 1) Duration queuePatience, // ...and this long in the waiting room, then leaves
        @NotNull Duration maxLeadTime) {                         // a crowd may be sent this long before a sale starts
}
