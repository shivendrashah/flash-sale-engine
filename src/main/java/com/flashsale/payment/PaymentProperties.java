package com.flashsale.payment;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The settings under "flashsale.payments" in application.yml.
 */
@ConfigurationProperties("flashsale.payments")
public record PaymentProperties(
        Duration fakeLatency,                   // flashsale.payments.fake-latency: how long the fake provider takes
        Duration gatewayTimeout,                // ...gateway-timeout: how long we wait for a provider's answer
        CircuitBreakerSettings circuitBreaker,  // ...circuit-breaker.*
        boolean recoveryJobEnabled,             // ...recovery-job-enabled (tests switch it off and call it directly)
        Duration recoveryJobInterval,           // ...recovery-job-interval
        Duration recoverAfter,                  // ...recover-after: a PENDING payment older than this is "stuck"
        int recoveryBatchSize) {                // ...recovery-batch-size

    /** One circuit breaker per payment provider, all with these rules (PaymentResilienceConfig). */
    public record CircuitBreakerSettings(
            int failureRateThreshold,           // % of failed calls that opens the circuit
            int slidingWindowSize,              // judge by the last N calls
            int minimumNumberOfCalls,           // ...once there are at least this many
            Duration waitInOpenState,           // stay OPEN this long, then try a few calls (HALF_OPEN)
            int permittedCallsInHalfOpenState) {
    }

    /** Seconds a client should wait before trying again while a provider is down (the Retry-After header). */
    public int retryAfterSeconds() {
        return (int) Math.max(1, (circuitBreaker.waitInOpenState().toMillis() + 999) / 1000); // rounded up
    }
}
