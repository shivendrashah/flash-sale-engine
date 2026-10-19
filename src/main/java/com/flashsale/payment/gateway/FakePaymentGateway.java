package com.flashsale.payment.gateway;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.flashsale.common.Pause;

/**
 * A pretend payment gateway for local runs and tests (no real money, no network). It plays both sides of a
 * real integration: the provider (which remembers every charge by its idempotency key) and our HTTP client
 * (which waits at most flashsale.payments.gateway-timeout for an answer).
 * Test tokens:
 *   tok_success      approved
 *   tok_declined     declined (card_declined); any unknown token is declined as invalid_payment_token
 *   tok_timeout      the provider CHARGES the card, but its answer comes too late: we time out (unknown)
 *   tok_lost         the request never reaches the provider: we time out, and nothing was charged (unknown)
 *   tok_unavailable  the provider answers "503, try later" at once: nothing was charged (known)
 */
abstract class FakePaymentGateway implements PaymentGateway {

    /** "Longer than anyone waits": the answer for tok_timeout and tok_lost. */
    private static final Duration NEVER = ChronoUnit.FOREVER.getDuration();

    // The provider's records: every answer, by idempotency key, like a real gateway keeps them.
    private final Map<String, ChargeResult> resultsByIdempotencyKey = new ConcurrentHashMap<>();
    private final Duration latency;
    private final Duration timeout;

    /**
     * @param latency how long the provider normally takes to answer (flashsale.payments.fake-latency)
     * @param timeout how long our side waits for an answer (flashsale.payments.gateway-timeout)
     */
    protected FakePaymentGateway(Duration latency, Duration timeout) {
        this.latency = latency;
        this.timeout = timeout;
    }

    @Override
    public final ChargeResult charge(ChargeRequest request) {
        // 1. Same key, same answer, and the card is charged only once.
        ChargeResult earlier = resultsByIdempotencyKey.get(request.idempotencyKey());
        if (earlier != null) {
            return answerAfter(latency, earlier);
        }
        String token = request.paymentToken();
        if ("tok_unavailable".equals(token)) {
            // 2a. The provider is down and says so at once. Nothing is charged, and we know it.
            Pause.of(latency);
            throw new GatewayUnavailableException(provider(), "the provider answered 503 Service Unavailable");
        }
        if ("tok_lost".equals(token)) {
            // 2b. The request is lost on the way: the provider never sees it, so no answer will ever come.
            return answerAfter(NEVER, null); // waits for the timeout, then throws GatewayTimeoutException
        }
        ChargeResult result = decide(request); // 3. the provider decides, by test token
        // 4. The provider records the answer. putIfAbsent is atomic: if two threads raced with the same key, the
        //    second one gets the first one's answer. (A get-then-put would be a "check-then-act" race.)
        ChargeResult racedResult = resultsByIdempotencyKey.putIfAbsent(request.idempotencyKey(), result);
        ChargeResult recorded = racedResult != null ? racedResult : result;
        // 5. The answer travels back to us. With tok_timeout it takes longer than we are willing to wait, so the
        //    card IS charged but we never hear about it: exactly the "did it charge or not?" problem (HLD.md 12).
        return answerAfter("tok_timeout".equals(token) ? NEVER : latency, recorded);
    }

    /**
     * records (Stripe: retrieve the PaymentIntent). Empty = it never received that charge.
     */
    @Override
    public final Optional<ChargeResult> lookup(String idempotencyKey) {
        return Optional.ofNullable(answerAfter(latency, resultsByIdempotencyKey.get(idempotencyKey)));
    }

    /**
     * Our side of the call: the answer arrives after {@code responseTime}, unless that is longer than the timeout.
     */
    private ChargeResult answerAfter(Duration responseTime, ChargeResult answer) {
        if (responseTime.compareTo(timeout) > 0) {
            Pause.of(timeout);
            throw new GatewayTimeoutException(provider(), timeout);
        }
        Pause.of(responseTime);
        return answer;
    }

    private ChargeResult decide(ChargeRequest request) {
        return switch (request.paymentToken()) {
            case "tok_success", "tok_timeout" -> ChargeResult.approved(referencePrefix() + UUID.randomUUID()
                    .toString().replace("-", "").substring(0, 16));
            case "tok_declined" -> ChargeResult.declined("card_declined");
            default -> ChargeResult.declined("invalid_payment_token");
        };
    }

    /** How this provider's payment ids start, e.g. "pi_" for Stripe. The "hook" each subclass fills in. */
    protected abstract String referencePrefix();
}
