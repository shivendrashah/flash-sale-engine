package com.flashsale;

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

import com.jayway.jsonpath.JsonPath;
import com.flashsale.inventory.RedisLock;
import com.flashsale.support.IntegrationTest;

/**
 * Tests the HTTP layer: URLs, status codes, headers, JSON shape and the error format.
 */
class ApiTest extends IntegrationTest {

    @Autowired
    MockMvc mvc;              // provided by @AutoConfigureMockMvc on IntegrationTest

    @Autowired
    RedisLock redisLock;

    @Test
    void aFullPurchaseOverHttp() throws Exception {
        long saleItemId = saleItemWithStock(3);

        // create a hold over HTTP
        String holdJson = mvc.perform(createHold("alice", "hold-1", saleItemId))
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotent-Replayed", "false"))
                .andExpect(jsonPath("$.status").value("HELD"))
                .andExpect(jsonPath("$.items[0].quantity").value(1))
                .andReturn().getResponse().getContentAsString();
        long holdId = JsonPath.<Number>read(holdJson, "$.holdId").longValue(); // read one field out of the JSON

        // pay for it
        String paymentJson = mvc.perform(post("/api/holds/{holdId}/pay", holdId)
                        .header("X-User-Id", "alice")
                        .header("Idempotency-Key", "pay-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"provider": "STRIPE", "paymentToken": "tok_success"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paymentStatus").value("SUCCEEDED"))
                .andExpect(jsonPath("$.holdStatus").value("CONFIRMED"))
                .andReturn().getResponse().getContentAsString();
        long orderId = JsonPath.<Number>read(paymentJson, "$.orderId").longValue();

        // read the order
        mvc.perform(get("/api/orders/{orderId}", orderId).header("X-User-Id", "alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.holdId").value(holdId))
                .andExpect(jsonPath("$.status").value("CONFIRMED"));
    }

    @Test
    void retryingAHoldRequestReturns200WithTheSameHold() throws Exception {
        long saleItemId = saleItemWithStock(3);
        String first = mvc.perform(createHold("alice", "hold-1", saleItemId))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long holdId = JsonPath.<Number>read(first, "$.holdId").longValue();

        mvc.perform(createHold("alice", "hold-1", saleItemId))   // same Idempotency-Key again
                .andExpect(status().isOk())
                .andExpect(header().string("Idempotent-Replayed", "true"))
                .andExpect(jsonPath("$.holdId").value(holdId));
    }

    @Test
    void aHoldRequestWithoutAnIdempotencyKeyIsRejected() throws Exception {
        long saleItemId = saleItemWithStock(3);

        mvc.perform(post("/api/holds")
                        .header("X-User-Id", "alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(holdBody(saleItemId)))
                .andExpect(status().isBadRequest()) // stopped: the controller method never ran
                // Spring's own error, with our code added (GlobalExceptionHandler.handleExceptionInternal)
                .andExpect(jsonPath("$.code").value("MISSING_HEADER"))
                .andExpect(jsonPath("$.detail").value("Required header 'Idempotency-Key' is not present."));
    }

    @Test
    void soldOutIsA409WithAMachineReadableCode() throws Exception {
        long saleItemId = saleItemWithStock(1);
        mvc.perform(createHold("alice", "hold-1", saleItemId)).andExpect(status().isCreated());

        mvc.perform(createHold("bob", "hold-1", saleItemId))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.code").value("SOLD_OUT"))
                .andExpect(jsonPath("$.status").value(409));
    }

    @Test
    void invalidRequestsSayWhichFieldIsWrong() throws Exception {
        long saleItemId = saleItemWithStock(3);

        mvc.perform(post("/api/holds")
                        .header("X-User-Id", "alice")
                        .header("Idempotency-Key", "hold-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"items": [{"saleItemId": %d, "quantity": 1}, {"saleItemId": %d, "quantity": 1}]}
                                """.formatted(saleItemId, saleItemId)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.errors[0]").value("eachItemListedOnce: each saleItemId may appear only once"));
    }

    @Test
    void aBusyItemIsA503WithRetryAfter() throws Exception {
        long saleItemId = saleItemWithStock(3);
        // Someone else holds the item's Redis lock for longer than a buyer is willing to wait (3 s).
        redisLock.tryAcquire("lock:sale-item:" + saleItemId, Duration.ofSeconds(30), Duration.ZERO).orElseThrow();

        mvc.perform(createHold("alice", "hold-1", saleItemId).header("X-Concurrency-Strategy", "REDIS_LOCK"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.code").value("STOCK_BUSY"));
        assertThat(availableStock(saleItemId)).as("nothing was taken").isEqualTo(3);
    }

    @Test
    void anUnknownConcurrencyStrategyIsABadRequest() throws Exception {
        long saleItemId = saleItemWithStock(3);

        // Spring can't turn "FASTEST" into the ConcurrencyStrategy enum, so it answers 400 by itself.
        mvc.perform(createHold("alice", "hold-1", saleItemId).header("X-Concurrency-Strategy", "FASTEST"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_VALUE"));
    }

    @Test
    void theSalePageShowsHowMuchIsLeft() throws Exception {
        long saleItemId = saleItemWithStock(3);
        mvc.perform(createHold("alice", "hold-1", saleItemId)).andExpect(status().isCreated());

        mvc.perform(get("/api/sales/{saleId}", 1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.active").value(true))
                .andExpect(jsonPath("$.items[0].totalStock").value(3))
                .andExpect(jsonPath("$.items[0].availableStock").value(2));
    }

    @Test
    void anAdminCanCreateASale() throws Exception {
        Instant now = clock.instant();
        mvc.perform(post("/api/admin/sales")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "Diwali Flash Sale",
                                  "startsAt": "%s",
                                  "endsAt": "%s",
                                  "items": [
                                    {"productName": "iPhone 17", "regularPrice": 79999, "salePrice": 59999, "stock": 100}
                                  ]
                                }
                                """.formatted(now, now.plus(Duration.ofHours(1)))))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.items[0].availableStock").value(100));
    }

    @Test
    void aSaleThatEndsBeforeItStartsIsRejected() throws Exception {
        Instant now = clock.instant();
        mvc.perform(post("/api/admin/sales")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name": "Backwards", "startsAt": "%s", "endsAt": "%s",
                                 "items": [{"productName": "x", "regularPrice": 1, "salePrice": 1, "stock": 1}]}
                                """.formatted(now, now.minus(Duration.ofHours(1)))))
                .andExpect(status().isBadRequest()); // CreateSaleRequest.isWindowValid() is false
    }

    @Test
    void aPartialCartListsWhatItCouldNotHold() throws Exception {
        long[] ids = saleItemsWithStock(5, 0);

        String json = mvc.perform(post("/api/holds")
                        .header("X-User-Id", "alice")
                        .header("Idempotency-Key", "cart-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"items": [{"saleItemId": %d, "quantity": 1}, {"saleItemId": %d, "quantity": 1}],
                                 "mode": "PARTIAL"}
                                """.formatted(ids[0], ids[1])))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.items[0].saleItemId").value(ids[0]))
                .andExpect(jsonPath("$.items[1]").doesNotExist())
                .andExpect(jsonPath("$.unavailable[0].saleItemId").value(ids[1]))
                .andExpect(jsonPath("$.unavailable[0].quantity").value(1))
                .andReturn().getResponse().getContentAsString();
        long holdId = JsonPath.<Number>read(json, "$.holdId").longValue();

        // GET shows the hold as stored. "unavailable" was part of the answer to the cart, so it is left out here
        // (@JsonInclude(NON_EMPTY) on HoldView.unavailable).
        mvc.perform(get("/api/holds/{holdId}", holdId).header("X-User-Id", "alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.unavailable").doesNotExist());
    }

    @Test
    void itemsFromDifferentSalesAreA422() throws Exception {
        long first = saleItemWithStock(5);    // each call creates its own sale
        long second = saleItemWithStock(5);

        mvc.perform(post("/api/holds")
                        .header("X-User-Id", "alice")
                        .header("Idempotency-Key", "cart-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"items": [{"saleItemId": %d, "quantity": 1}, {"saleItemId": %d, "quantity": 1}]}
                                """.formatted(first, second)))
                .andExpect(status().is(422))   // well-formed JSON, but the cart makes no sense
                .andExpect(jsonPath("$.code").value("MIXED_SALES"));
    }

    @Test
    void theStockGateCanBeSwitchedOffWithAHeader() throws Exception {
        long saleItemId = saleItemWithStock(3);

        mvc.perform(createHold("alice", "hold-1", saleItemId).header("X-Stock-Gate", "off"))
                .andExpect(status().isCreated());
        assertThat(gateCounter(saleItemId)).as("Redis was never asked").isNull();

        mvc.perform(createHold("bob", "hold-2", saleItemId))   // no header: the gate follows application.yml (on)
                .andExpect(status().isCreated());
        assertThat(gateCounter(saleItemId)).as("seeded with PostgreSQL's 2, minus bob's 1").isEqualTo(1);
    }

    @Test
    void anUnknownStockGateValueIsABadRequest() throws Exception {
        long saleItemId = saleItemWithStock(3);

        mvc.perform(createHold("alice", "hold-1", saleItemId).header("X-Stock-Gate", "maybe"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_VALUE"));
    }

    @Test
    void brokenJsonIsA400WithACode() throws Exception {
        mvc.perform(post("/api/holds")
                        .header("X-User-Id", "alice")
                        .header("Idempotency-Key", "hold-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"items\": [{\"saleItemId\": 1,"))          // cut off half way
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.code").value("UNREADABLE_BODY"));
    }

    @Test
    void aWrongMethodOrAnUnknownUrlHasACodeToo() throws Exception {
        mvc.perform(get("/api/holds").header("X-User-Id", "alice"))   // there is no "list all holds" route
                .andExpect(status().isMethodNotAllowed())
                .andExpect(jsonPath("$.code").value("METHOD_NOT_ALLOWED"));
        mvc.perform(get("/api/nothing-here"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("NOT_FOUND"));
    }

    @Test
    void actuatorCountsWhatTheGateRefused() throws Exception {
        long saleItemId = saleItemWithStock(1);
        mvc.perform(createHold("alice", "hold-1", saleItemId)).andExpect(status().isCreated());
        mvc.perform(createHold("bob", "hold-2", saleItemId)).andExpect(status().isConflict());  // Redis said no

        mvc.perform(get("/actuator/metrics/flashsale.stock.gate").param("tag", "outcome:refused"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.measurements[0].value").value(greaterThanOrEqualTo(1.0)));
    }

    @Test
    void aPaymentWhoseAnswerComesTooLateIsA202PointingAtTheHold() throws Exception {
        long holdId = holdOverHttp("alice", saleItemWithStock(3));

        mvc.perform(payOverHttp("alice", holdId, "tok_timeout"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/api/holds/" + holdId))   // watch the hold for the result
                .andExpect(jsonPath("$.paymentStatus").value("PENDING"))
                .andExpect(jsonPath("$.holdStatus").value("PAYMENT_PENDING"));
    }

    @Test
    void aPaymentProviderOutageIsA503WithRetryAfter() throws Exception {
        long holdId = holdOverHttp("alice", saleItemWithStock(3));

        mvc.perform(payOverHttp("alice", holdId, "tok_unavailable"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.code").value("PAYMENT_PROVIDER_UNAVAILABLE"));
    }

    @Test
    void actuatorShowsEachPaymentProvidersCircuitBreaker() throws Exception {
        mvc.perform(get("/actuator/metrics/resilience4j.circuitbreaker.state")
                        .param("tag", "name:STRIPE").param("tag", "state:closed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.measurements[0].value").value(1.0));
    }

    @Test
    void aSaleCanLimitUnitsPerBuyer() throws Exception {
        String saleJson = mvc.perform(post("/api/admin/sales")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name": "Sneaker Drop", "startsAt": "2026-01-01T00:00:00Z",
                                 "endsAt": "2030-01-01T00:00:00Z", "perUserLimit": 1,
                                 "items": [{"productName": "Sneakers", "regularPrice": 200, "salePrice": 99,
                                            "stock": 10}]}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.perUserLimit").value(1))
                .andReturn().getResponse().getContentAsString();
        long saleItemId = JsonPath.<Number>read(saleJson, "$.items[0].saleItemId").longValue();

        mvc.perform(createHold("alice", "hold-1", saleItemId)).andExpect(status().isCreated());
        mvc.perform(createHold("alice", "hold-2", saleItemId))       // a second hold: over the limit of 1
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("PURCHASE_LIMIT_EXCEEDED"));
    }

    @Test
    void aPerUserLimitBelowOneIsABadRequest() throws Exception {
        mvc.perform(post("/api/admin/sales")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name": "Broken", "startsAt": "2026-01-01T00:00:00Z", "endsAt": "2030-01-01T00:00:00Z",
                                 "perUserLimit": 0,
                                 "items": [{"productName": "X", "regularPrice": 1, "salePrice": 1, "stock": 1}]}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]").value("perUserLimit: must be greater than 0"));
    }

    /** POST /api/holds for one unit; returns the new hold's id. */
    private long holdOverHttp(String userId, long saleItemId) throws Exception {
        String json = mvc.perform(createHold(userId, userId + "-hold", saleItemId))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return JsonPath.<Number>read(json, "$.holdId").longValue();
    }

    private MockHttpServletRequestBuilder payOverHttp(String userId, long holdId, String paymentToken) {
        return post("/api/holds/{holdId}/pay", holdId)
                .header("X-User-Id", userId)
                .header("Idempotency-Key", userId + "-pay")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"provider": "STRIPE", "paymentToken": "%s"}
                        """.formatted(paymentToken));
    }

    /** Builds the request curl would send: POST /api/holds with headers and a JSON body. */
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
