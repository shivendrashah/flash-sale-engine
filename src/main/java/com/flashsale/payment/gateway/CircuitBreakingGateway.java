package com.flashsale.payment.gateway;

import java.util.Optional;
import java.util.function.Supplier;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import com.flashsale.payment.PaymentProvider;

/**
 * A circuit breaker in front of one payment provider. While the provider keeps failing (timeouts, 503s), the
 * breaker "opens" and calls fail at once without being sent: threads stop piling up behind a provider that is
 * down, and the provider gets room to recover. After a pause it lets a few trial calls through ("half-open");
 * if they work it closes again, if not it opens again.
 *   CLOSED --(failure rate >= threshold)--> OPEN --(wait-in-open-state)--> HALF_OPEN --(trials OK)--> CLOSED
 *                                            ^------------------(trials fail)-------------'
 */
final class CircuitBreakingGateway implements PaymentGateway {

    private final PaymentGateway provider;
    private final CircuitBreaker breaker;

    CircuitBreakingGateway(PaymentGateway provider, CircuitBreaker breaker) {
        this.provider = provider;
        this.breaker = breaker;
    }

    @Override
    public PaymentProvider provider() {
        return provider.provider();
    }

    @Override
    public ChargeResult charge(ChargeRequest request) {
        return throughBreaker(() -> provider.charge(request));
    }

    @Override
    public Optional<ChargeResult> lookup(String idempotencyKey) {
        return throughBreaker(() -> provider.lookup(idempotencyKey));
    }

    private <T> T throughBreaker(Supplier<T> call) {
        try {
            return breaker.executeSupplier(call);
        } catch (CallNotPermittedException open) {
            // The breaker is OPEN: the call was never sent, so nothing was charged. Same as the provider saying 503.
            throw new GatewayUnavailableException(provider(), "circuit breaker is open: call not sent");
        }
    }
}
