package com.flashsale.waitingroom;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;

import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The settings under "flashsale.waiting-room". How many buyers a second is set per sale (admitPerSecond);
 * these say how often the admitter runs and how long a pass lasts.
 */
@ConfigurationProperties("flashsale.waiting-room")
@Validated
public record WaitingRoomProperties(
        boolean admitterEnabled,                                       // false = no WaitingRoomScheduler (tests)
        @NotNull @DurationMin(millis = 1) Duration admitInterval,      // how often the admitter runs
        @NotNull @DurationMin(millis = 1) Duration passTtl) {          // how long an admitted buyer may shop
}
