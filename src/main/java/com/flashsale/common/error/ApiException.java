package com.flashsale.common.error;

import org.springframework.http.HttpStatus;

/**
 * The parent class of all our business errors (sold out, hold expired, not found...).
 * Each subclass fixes its HTTP status (like 409) and a stable machine-readable code (like "SOLD_OUT").
 *
 *        (By default it does NOT roll back on checked exceptions.)
 */
public abstract class ApiException extends RuntimeException {

    private final HttpStatus status;
    private final String code;

    protected ApiException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public HttpStatus status() {
        return status;
    }

    public String code() {
        return code;
    }

    /** Seconds the client should wait before retrying, or null if retrying won't help. */
    public Integer retryAfterSeconds() {
        return null;
    }
}
