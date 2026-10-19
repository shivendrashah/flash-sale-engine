package com.flashsale.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.json.JsonMapper;

/**
 * What the sale-page cache does when Redis is unreachable or too slow: the page comes from PostgreSQL, and
 * Redis is tried only ONCE per page view (a hanging Redis costs one timeout, not three). Switched off, it never
 * calls Redis.
 */
class SaleCacheFailureTest {

    private static final SaleCacheProperties ON = new SaleCacheProperties(true, Duration.ofSeconds(1));
    private static final Instant NOW = Instant.parse("2026-10-09T10:00:00Z");
    private static final SaleView PAGE = new SaleView(1, "Diwali Sale", NOW.minusSeconds(60), NOW.plusSeconds(3600),
            true, null, null, List.of());

    private final SaleService saleService = mock(SaleService.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void ifRedisIsDownThePageComesFromPostgres() {
        StringRedisTemplate redis = redisThatFails(new RedisConnectionFailureException("Redis is down"));
        when(saleService.getSale(1)).thenReturn(PAGE);

        SaleView page = cache(redis, ON).getSale(1);

        assertThat(page).isEqualTo(PAGE);
        assertThat(meters.counter("flashsale.sale.cache", "outcome", "redis_down").count()).isEqualTo(1);
        assertThat(meters.counter("flashsale.sale.cache", "outcome", "miss").count()).isEqualTo(1);
    }

    @Test
    void ifRedisIsTooSlowItIsAskedOnlyOncePerPageView() {
        StringRedisTemplate redis = redisThatFails(new QueryTimeoutException("Redis command timed out"));
        when(saleService.getSale(1)).thenReturn(PAGE);

        cache(redis, ON).getSale(1);

        assertThat(mockingDetails(redis).getInvocations()).as("calls to Redis").hasSize(1);
    }

    @Test
    void switchedOffItNeverCallsRedis() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(saleService.getSale(1)).thenReturn(PAGE);

        assertThat(cache(redis, new SaleCacheProperties(false, Duration.ofSeconds(1))).getSale(1)).isEqualTo(PAGE);
        verifyNoInteractions(redis);
    }

    private SaleCache cache(StringRedisTemplate redis, SaleCacheProperties properties) {
        return new SaleCache(saleService, redis, JsonMapper.builder().build(), properties, clock, meters);
    }

    /** A fake StringRedisTemplate where EVERY method throws the given exception. */
    private static StringRedisTemplate redisThatFails(RuntimeException failure) {
        return mock(StringRedisTemplate.class, invocation -> {
            throw failure;
        });
    }
}
