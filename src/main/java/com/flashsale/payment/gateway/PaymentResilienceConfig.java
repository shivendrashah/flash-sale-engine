package com.flashsale.payment.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.payment.PaymentProperties;

/**
 * Builds the circuit breakers for the payment providers from the settings under
 * flashsale.payments.circuit-breaker in application.yml, and publishes their state as metrics.
 */
@Configuration(proxyBeanMethods = false)
class PaymentResilienceConfig {

    private static final Logger log = LoggerFactory.getLogger(PaymentResilienceConfig.class);

    @Bean
    CircuitBreakerRegistry paymentCircuitBreakers(PaymentProperties properties, MeterRegistry meters) {
        PaymentProperties.CircuitBreakerSettings settings = properties.circuitBreaker();
        CircuitBreakerConfig rules = CircuitBreakerConfig.custom()
                // judge the provider by its last N calls ...
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(settings.slidingWindowSize())
                // ... but only once there are enough calls to judge (2 failures out of 2 calls proves little)
                .minimumNumberOfCalls(settings.minimumNumberOfCalls())
                .failureRateThreshold(settings.failureRateThreshold())          // e.g. 50 (%) -> OPEN
                .waitDurationInOpenState(settings.waitInOpenState())             // OPEN this long -> HALF_OPEN
                .permittedNumberOfCallsInHalfOpenState(settings.permittedCallsInHalfOpenState())
                .recordExceptions(GatewayException.class)
                .build();
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(rules);
        // Log every state change, e.g. "Circuit breaker STRIPE: CLOSED -> OPEN": the first thing to look for when
        // payments start failing fast. Each breaker publishes events; we subscribe as it is created.
        registry.getEventPublisher().onEntryAdded(added -> added.getAddedEntry().getEventPublisher()
                .onStateTransition(event -> log.warn("Circuit breaker {}: {}", event.getCircuitBreakerName(),
                        event.getStateTransition())));
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meters);
        return registry;
    }
}
