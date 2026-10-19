package com.flashsale.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.common.error.NotFoundException;
import com.flashsale.support.Concurrently;
import com.flashsale.support.IntegrationTest;

/**
 * The sale-page cache: a miss reads PostgreSQL and stores a copy for 1 s; a hit doesn't touch PostgreSQL;
 * "active" is always worked out fresh; a crowd that misses at once reads PostgreSQL ONCE; a missing sale or a
 * broken copy is handled; buying never trusts the cached page.
 */
class SaleCacheTest extends IntegrationTest {

    @Autowired
    SaleCache saleCache;

    @Autowired
    MeterRegistry meters;

    @Autowired
    MockMvc mvc;

    @Test
    void theFirstReadComesFromPostgresAndIsStoredForOneSecond() {
        long saleId = liveSaleWithStock(3).id();
        double misses = served("miss");

        SaleView page = saleCache.getSale(saleId);

        assertThat(page.items().getFirst().availableStock()).isEqualTo(3);
        assertThat(served("miss") - misses).as("read from PostgreSQL").isEqualTo(1);
        assertThat(redis.opsForValue().get(SaleCache.key(saleId))).contains("\"productName\":\"iPhone\"");
        assertThat(redis.getExpire(SaleCache.key(saleId), TimeUnit.MILLISECONDS)).isBetween(1L, 1000L);
    }

    @Test
    void theNextReadComesFromRedisWithoutTouchingPostgres() {
        long saleId = liveSaleWithStock(3).id();
        saleCache.getSale(saleId);
        // Change PostgreSQL behind the cache's back. If the next read went to PostgreSQL, it would see 0.
        jdbc.update("UPDATE sale_items SET available_stock = 0");
        double hits = served("hit");

        SaleView page = saleCache.getSale(saleId);

        assertThat(page.items().getFirst().availableStock()).as("the cached number").isEqualTo(3);
        assertThat(served("hit") - hits).isEqualTo(1);
    }

    @Test
    void onceTheCopyExpiresTheNextReadSeesPostgresAgain() {
        long saleId = liveSaleWithStock(3).id();
        saleCache.getSale(saleId);
        holdFor("alice", liveSaleItemId(saleId), 1);   // a real purchase: PostgreSQL now says 2

        assertThat(saleCache.getSale(saleId).items().getFirst().availableStock()).as("up to 1 s stale").isEqualTo(3);
        redis.delete(SaleCache.key(saleId));            // what Redis does by itself once the second is up

        assertThat(saleCache.getSale(saleId).items().getFirst().availableStock()).isEqualTo(2);
    }

    @Test
    void aStalePageNeverSellsWhatIsGone() {
        long saleId = liveSaleWithStock(1).id();
        long saleItemId = liveSaleItemId(saleId);
        assertThat(saleCache.getSale(saleId).items().getFirst().availableStock()).isEqualTo(1);
        holdFor("alice", saleItemId, 1);                // the last unit goes

        assertThat(saleCache.getSale(saleId).items().getFirst().availableStock()).as("the page still says 1")
                .isEqualTo(1);
        // ...but bob's hold checks stock in Redis's gate and PostgreSQL, not this page
        assertThatThrownBy(() -> holdFor("bob", saleItemId, 1)).isInstanceOf(SoldOutException.class);
    }

    @Test
    void activeIsWorkedOutOnEveryReadEvenFromTheCache() {
        Instant now = clock.instant();
        long saleId = saleService.createSale(new CreateSaleRequest("Starts Soon", now.plus(Duration.ofSeconds(30)),
                now.plus(Duration.ofHours(1)), List.of(iphone(5)))).id();
        assertThat(saleCache.getSale(saleId).active()).isFalse();

        clock.advance(Duration.ofSeconds(31));           // the sale starts; the copy in Redis was made before
        double hits = served("hit");

        assertThat(saleCache.getSale(saleId).active()).as("not 'not live yet' from the old copy").isTrue();
        assertThat(served("hit") - hits).as("still served from Redis").isEqualTo(1);
    }

    @Test
    void aCrowdThatMissesAtOnceReadsPostgresOnlyOnce() {
        long saleId = liveSaleWithStock(100).id();
        double misses = served("miss");

        List<Concurrently.Outcome<SaleView>> pages = Concurrently.run(50, i -> saleCache.getSale(saleId));

        assertThat(Concurrently.failures(pages)).isEmpty();
        assertThat(pages).allSatisfy(page -> assertThat(page.value().items().getFirst().availableStock())
                .isEqualTo(100));
        assertThat(served("miss") - misses).as("PostgreSQL reads for 50 page views").isEqualTo(1);
    }

    @Test
    void anUnknownSaleIsNotFoundAndNothingIsCached() {
        assertThatThrownBy(() -> saleCache.getSale(999)).isInstanceOf(NotFoundException.class);
        assertThat(redis.hasKey(SaleCache.key(999))).isFalse();
        // and everyone in a crowd gets the same 404, not a hang or a 500
        List<Concurrently.Outcome<SaleView>> pages = Concurrently.run(20, i -> saleCache.getSale(999));
        assertThat(Concurrently.failures(pages)).hasSize(20).allMatch(e -> e instanceof NotFoundException);
    }

    @Test
    void aCopyThatCantBeReadIsReplacedByAFreshOne() {
        long saleId = liveSaleWithStock(3).id();
        // e.g. written by an older version of the app with a different SaleView
        redis.opsForValue().set(SaleCache.key(saleId), "{\"id\": \"not a number\"");

        assertThat(saleCache.getSale(saleId).items().getFirst().availableStock()).isEqualTo(3);
        assertThat(redis.opsForValue().get(SaleCache.key(saleId))).contains("\"availableStock\":3");
    }

    @Test
    void theSalePageOverHttpComesFromTheCache() throws Exception {
        long saleId = liveSaleWithStock(3).id();
        mvc.perform(get("/api/sales/{saleId}", saleId)).andExpect(status().isOk());
        jdbc.update("UPDATE sale_items SET available_stock = 0");

        mvc.perform(get("/api/sales/{saleId}", saleId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.active").value(true))
                .andExpect(jsonPath("$.items[0].availableStock").value(3))
                .andExpect(jsonPath("$.perUserLimit").doesNotExist());   // still left out when there is no limit
    }

    /** Page reads served so far with this outcome (hit, miss, shared, redis_down). */
    private double served(String outcome) {
        return meters.counter("flashsale.sale.cache", "outcome", outcome).count();
    }

    private long liveSaleItemId(long saleId) {
        return saleService.getSale(saleId).items().getFirst().saleItemId();
    }
}
