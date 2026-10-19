package com.flashsale.ratelimit;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

/**
 * This buyer's token bucket is empty. Not a "no" to the purchase: the same request, sent again after
 * Retry-After seconds, is handled normally.
 */
public class RateLimitedException extends ApiException {

    private final int retryAfterSeconds;

    public RateLimitedException(int retryAfterSeconds) {
        super(HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMITED",
                "Too many requests. Wait " + retryAfterSeconds + " second(s) and try again");
        this.retryAfterSeconds = retryAfterSeconds;
    }

    @Override
    public Integer retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
