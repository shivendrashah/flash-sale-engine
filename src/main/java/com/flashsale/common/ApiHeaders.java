package com.flashsale.common;

/**
 * The names of the HTTP headers our API uses, in one place, so controllers can't misspell them.
 *
 *        object of it) is a common way to group constants.
 */
public final class ApiHeaders {

    /** Who is calling. In production this would come from a verified login token (JWT), not a plain header. */
    public static final String USER_ID = "X-User-Id";

    /** A unique id the client picks per action. Sending it again means "this is a retry, not a new request". */
    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    /** Optional, for demos and benchmarks: which concurrency strategy to use for this request. */
    public static final String CONCURRENCY_STRATEGY = "X-Concurrency-Strategy";

    /** Optional, for demos and benchmarks: "off" sends this request past the Redis stock gate (StockGate). */
    public static final String STOCK_GATE = "X-Stock-Gate";

    /** Response header: true when the response is a replay of an earlier request with the same Idempotency-Key. */
    public static final String IDEMPOTENT_REPLAYED = "Idempotent-Replayed";

    /** Response header: the size of the buyer's token bucket. */
    public static final String RATE_LIMIT_LIMIT = "X-RateLimit-Limit";

    /** Response header: whole tokens left in the buyer's bucket after this request. */
    public static final String RATE_LIMIT_REMAINING = "X-RateLimit-Remaining";

    private ApiHeaders() {
    }
}
