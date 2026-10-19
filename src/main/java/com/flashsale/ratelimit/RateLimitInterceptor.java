package com.flashsale.ratelimit;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import com.flashsale.common.ApiHeaders;

/**
 * Spends one of the buyer's tokens on every request that changes something (POST, PUT, PATCH, DELETE under
 * /api, except /api/admin). An empty bucket stops the request with 429 RATE_LIMITED, before the body is read
 * and before any database work. Every counted response says how much is left:
 *   X-RateLimit-Limit: 10        the bucket's size
 *   X-RateLimit-Remaining: 7     whole tokens left after this request
 * Reads (GET) are not counted: they change nothing, and step 6 puts the sale page in a cache.
 *
 * Why an interceptor and not a servlet filter? A filter runs earlier (before Spring MVC), so its errors skip
 * GlobalExceptionHandler and the 429 would need its own JSON code. An interceptor runs inside Spring MVC: it throws
 * RateLimitedException, and the 429 gets the same JSON shape as every other error.
 */
@Component
class RateLimitInterceptor implements HandlerInterceptor {

    /** Longer user ids are refused by the controllers' @Size(max = 64) anyway; don't make Redis keys of them. */
    private static final int MAX_USER_ID_LENGTH = 64;

    private final RateLimiter rateLimiter;

    RateLimitInterceptor(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    /**
     * @return true = go on to the controller. (It never returns false: a refusal throws, so the error handler can
     *         write it.)
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (changesNothing(request)) {
            return true;
        }
        String userId = request.getHeader(ApiHeaders.USER_ID);
        if (userId == null || userId.isBlank() || userId.length() > MAX_USER_ID_LENGTH) {
            return true;   // the controller answers 400 for this, before any real work
        }
        RateLimiter.Decision decision = rateLimiter.tryAcquire(userId);
        if (decision.counted()) {
            response.setHeader(ApiHeaders.RATE_LIMIT_LIMIT, String.valueOf(decision.limit()));
            response.setHeader(ApiHeaders.RATE_LIMIT_REMAINING, String.valueOf(decision.remaining()));
        }
        if (!decision.allowed()) {
            throw new RateLimitedException(decision.retryAfterSeconds());
        }
        return true;
    }

    /** GET, HEAD and OPTIONS only read. */
    private static boolean changesNothing(HttpServletRequest request) {
        String method = request.getMethod();
        return HttpMethod.GET.matches(method) || HttpMethod.HEAD.matches(method)
                || HttpMethod.OPTIONS.matches(method);
    }
}
