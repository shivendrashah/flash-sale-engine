package com.flashsale.waitingroom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.flashsale.catalog.Sale;

/**
 * With Redis gone, the waiting room steps aside: every buyer is told they're in, and the pass check lets
 * holds through. PostgreSQL still decides every sale; only the load protection is lost.
 */
class WaitingRoomFailureTest {

    private static final Instant NOW = Instant.parse("2026-10-10T10:00:00Z");

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class, invocation -> {
        throw new RedisConnectionFailureException("Redis is down");
    });
    private final WaitingRoom waitingRoom = new WaitingRoom(redis,
            new WaitingRoomProperties(true, Duration.ofMillis(250), Duration.ofMinutes(5)), meters,
            Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void withRedisDownEveryBuyerIsLetThrough() {
        Sale sale = queuedSale();

        assertThat(waitingRoom.join(sale, "alice").state()).isEqualTo(QueueStatus.State.ADMITTED);
        assertThatCode(() -> waitingRoom.requirePass(sale, "bob")).doesNotThrowAnyException();
        assertThat(meters.counter("flashsale.waiting_room", "outcome", "redis_down").count()).isEqualTo(2);
    }

    private static Sale queuedSale() {
        Sale sale = new Sale("Queued", NOW.minusSeconds(60), NOW.plusSeconds(3600), null, 5, NOW);
        ReflectionTestUtils.setField(sale, "id", 5L);   // as if saved: PostgreSQL would have given it an id
        return sale;
    }
}
