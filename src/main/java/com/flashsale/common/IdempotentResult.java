package com.flashsale.common;

/**
 * A response body, plus whether it was replayed from an earlier request with the same Idempotency-Key.
 */
public record IdempotentResult<T>(T body, boolean replayed) {

    public static <T> IdempotentResult<T> created(T body) {
        return new IdempotentResult<>(body, false);
    }

    public static <T> IdempotentResult<T> replayed(T body) {
        return new IdempotentResult<>(body, true);
    }
}
