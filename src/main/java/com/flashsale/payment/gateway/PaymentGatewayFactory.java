package com.flashsale.payment.gateway;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import com.flashsale.payment.PaymentProvider;

/**
 * Hands out the gateway for a provider, wrapped in that provider's circuit breaker. Adding a provider =
 * adding one PaymentGateway class.
 */
@Component
public class PaymentGatewayFactory {

    private final Map<PaymentProvider, PaymentGateway> byProvider = new EnumMap<>(PaymentProvider.class);

    public PaymentGatewayFactory(List<PaymentGateway> gateways, CircuitBreakerRegistry circuitBreakers) {
        for (PaymentGateway gateway : gateways) {
            PaymentGateway guarded = new CircuitBreakingGateway(gateway,
                    circuitBreakers.circuitBreaker(gateway.provider().name()));
            if (byProvider.put(gateway.provider(), guarded) != null) {
                throw new IllegalStateException("Two gateways registered for " + gateway.provider());
            }
        }
    }

    public PaymentGateway get(PaymentProvider provider) {
        PaymentGateway gateway = byProvider.get(provider);
        if (gateway == null) {
            throw new IllegalStateException("No gateway registered for " + provider);
        }
        return gateway;
    }
}
