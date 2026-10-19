package com.flashsale.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.flashsale.support.IntegrationTest;

/**
 * The rate limiter as a client sees it: the X-RateLimit-* headers, a 429 with Retry-After and the
 * RATE_LIMITED code before any database work, and which requests are NOT counted (reads, the admin API,
 * requests without a user).
 */
class RateLimitApiTest extends IntegrationTest {

    @Autowired
    MockMvc mvc;

    @Test
    void everyCountedResponseShowsTheBudget() throws Exception {
        long saleItemId = saleItemWithStock(3);

        mvc.perform(createHold("alice", "hold-1", saleItemId))
                .andExpect(status().isCreated())
                .andExpect(header().string("X-RateLimit-Limit", "10"))
                .andExpect(header().string("X-RateLimit-Remaining", "9"));
    }

    @Test
    void anEmptyBucketIsA429WithRetryAfterAndNoDatabaseWork() throws Exception {
        long saleItemId = saleItemWithStock(3);
        emptyBucket("alice");

        mvc.perform(createHold("alice", "hold-1", saleItemId))
                .andExpect(status().isTooManyRequests())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.code").value("RATE_LIMITED"))
                .andExpect(header().string("Retry-After", "1"))          // the next token is ~200 ms away
                .andExpect(header().string("X-RateLimit-Remaining", "0"));

        assertThat(count("holds")).as("the controller never ran").isZero();
        assertThat(availableStock(saleItemId)).isEqualTo(3);
        assertThat(gateCounter(saleItemId)).as("the stock gate was never asked").isNull();
    }

    @Test
    void afterWaitingTheSameRequestGoesThrough() throws Exception {
        long saleItemId = saleItemWithStock(3);
        emptyBucket("alice");
        mvc.perform(createHold("alice", "hold-1", saleItemId)).andExpect(status().isTooManyRequests());

        ageBucket("alice", Duration.ofSeconds(1));     // "one second later"

        mvc.perform(createHold("alice", "hold-1", saleItemId))   // same Idempotency-Key: the 429 saved nothing
                .andExpect(status().isCreated());
    }

    @Test
    void oneBuyersEmptyBucketDoesNotSlowDownAnother() throws Exception {
        long saleItemId = saleItemWithStock(3);
        emptyBucket("alice");

        mvc.perform(createHold("bob", "hold-1", saleItemId)).andExpect(status().isCreated());
    }

    @Test
    void readsAreNotCounted() throws Exception {
        saleItemWithStock(3);
        emptyBucket("alice");

        mvc.perform(get("/api/sales/{saleId}", 1).header("X-User-Id", "alice"))
                .andExpect(status().isOk())
                .andExpect(header().doesNotExist("X-RateLimit-Remaining"));
        mvc.perform(get("/api/holds/{holdId}", 42).header("X-User-Id", "alice"))
                .andExpect(status().isNotFound());                // a real answer, not a 429
    }

    @Test
    void theAdminApiIsNotCounted() throws Exception {
        emptyBucket("alice");
        Instant now = clock.instant();

        mvc.perform(post("/api/admin/sales")
                        .header("X-User-Id", "alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name": "Staff Sale", "startsAt": "%s", "endsAt": "%s",
                                 "items": [{"productName": "x", "regularPrice": 1, "salePrice": 1, "stock": 1}]}
                                """.formatted(now, now.plus(Duration.ofHours(1)))))
                .andExpect(status().isCreated());
    }

    @Test
    void aRequestWithoutAUserIsRefusedByTheControllerAndNotCounted() throws Exception {
        long saleItemId = saleItemWithStock(3);

        mvc.perform(post("/api/holds")
                        .header("Idempotency-Key", "hold-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(holdBody(saleItemId)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("MISSING_HEADER"))
                .andExpect(header().doesNotExist("X-RateLimit-Remaining"));
    }

    @Test
    void actuatorCountsTheRefusedRequests() throws Exception {
        long saleItemId = saleItemWithStock(3);
        emptyBucket("alice");
        mvc.perform(createHold("alice", "hold-1", saleItemId)).andExpect(status().isTooManyRequests());

        mvc.perform(get("/actuator/metrics/flashsale.rate.limit").param("tag", "outcome:limited"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.measurements[0].value").value(greaterThanOrEqualTo(1.0)));
    }

    private MockHttpServletRequestBuilder createHold(String userId, String idempotencyKey, long saleItemId) {
        return post("/api/holds")
                .header("X-User-Id", userId)
                .header("Idempotency-Key", idempotencyKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(holdBody(saleItemId));
    }

    private static String holdBody(long saleItemId) {
        return """
                {"items": [{"saleItemId": %d, "quantity": 1}]}
                """.formatted(saleItemId);
    }
}
