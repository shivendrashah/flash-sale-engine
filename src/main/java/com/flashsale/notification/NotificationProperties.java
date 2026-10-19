package com.flashsale.notification;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The settings under "flashsale.notifications": how often, and how far apart, the worker retries a message
 * before it moves it to the dead-letter topic.
 */
@ConfigurationProperties("flashsale.notifications")
@Validated
public record NotificationProperties(
        @PositiveOrZero int maxRetries,      // flashsale.notifications.max-retries: tries after the first one
        @NotNull Duration retryBackoff) {    // ...retry-backoff: the pause before each retry
}
