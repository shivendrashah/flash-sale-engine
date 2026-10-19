package com.flashsale.waitingroom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.flashsale.catalog.CreateSaleRequest;
import com.flashsale.catalog.SaleNotActiveException;
import com.flashsale.catalog.SaleView;
import com.flashsale.common.error.NotFoundException;
import com.flashsale.support.Concurrently;
import com.flashsale.support.IntegrationTest;

/**
 * The waiting room:
 *   a sale without one sells as before; with one, a buyer without a pass is refused (403 NOT_ADMITTED)
 *   before anything is reserved;
 *   buyers queue in arrival order, and joining again keeps your place;
 *   the admitter lets in admitPerSecond buyers a second, first in line first, and only they can buy;
 *   four admitters at once (several app instances) still let in only one second's worth;
 *   a pass runs out; the line can form before the sale starts; the HTTP endpoints.
 */
class WaitingRoomTest extends IntegrationTest {

    @Autowired
    WaitingRoomService waitingRoom;

    @Autowired
    WaitingRoomAdmitter admitter;

    @Autowired
    MockMvc mvc;

    @Test
    void aSaleWithoutAWaitingRoomSellsAsBefore() {
        long saleItemId = saleItemWithStock(5);
        long saleId = saleIdOf(saleItemId);

        assertThat(waitingRoom.join(saleId, "alice").state()).isEqualTo(QueueStatus.State.ADMITTED);
        assertThat(holdFor("bob", saleItemId, 1).holdId()).as("bob never joined: no line, no pass").isPositive();
    }

    @Test
    void withoutAPassAHoldIsRefusedBeforeAnythingIsReserved() {
        SaleView sale = queuedSale(5, 10);
        long saleItemId = sale.items().getFirst().saleItemId();

        assertThatThrownBy(() -> holdFor("alice", saleItemId, 1)).isInstanceOf(NotAdmittedException.class);
        assertThat(availableStock(saleItemId)).isEqualTo(10);
        assertThat(gateCounter(saleItemId)).as("Redis's stock gate never asked").isNull();
    }

    @Test
    void buyersQueueInArrivalOrderAndJoiningAgainKeepsYourPlace() {
        long saleId = queuedSale(5, 10).id();

        assertThat(waitingRoom.join(saleId, "alice").position()).isEqualTo(1);
        assertThat(waitingRoom.join(saleId, "bob").position()).isEqualTo(2);
        assertThat(waitingRoom.join(saleId, "carol").position()).isEqualTo(3);
        assertThat(waitingRoom.join(saleId, "alice").position()).as("a double-click keeps the place").isEqualTo(1);

        QueueStatus carol = waitingRoom.status(saleId, "carol");
        assertThat(carol.state()).isEqualTo(QueueStatus.State.WAITING);
        assertThat(carol.estimatedWaitSeconds()).as("3rd in line, 5 a second").isEqualTo(1);
        assertThat(waitingRoom.status(saleId, "dave").state()).isEqualTo(QueueStatus.State.NOT_QUEUED);
    }

    @Test
    void theAdmitterLetsTheFirstInLineInAtTheSalesRate() {
        SaleView sale = queuedSale(5, 100);
        long saleItemId = sale.items().getFirst().saleItemId();
        List<String> buyers = IntStream.rangeClosed(1, 12).mapToObj(i -> "buyer-" + i).toList();
        buyers.forEach(buyer -> waitingRoom.join(sale.id(), buyer));

        assertThat(admitter.admitNext()).as("one second's worth").isEqualTo(5);

        assertThat(buyers.subList(0, 5)).allSatisfy(buyer ->
                assertThat(waitingRoom.status(sale.id(), buyer).state()).isEqualTo(QueueStatus.State.ADMITTED));
        assertThat(waitingRoom.status(sale.id(), "buyer-6").position()).as("now first in line").isEqualTo(1);
        assertThat(holdFor("buyer-1", saleItemId, 1).holdId()).isPositive();
        assertThatThrownBy(() -> holdFor("buyer-6", saleItemId, 1)).isInstanceOf(NotAdmittedException.class);

        assertThat(admitter.admitNext()).as("no credit left this second").isZero();
        oneSecondLater(sale.id());
        assertThat(admitter.admitNext()).isEqualTo(5);
        oneSecondLater(sale.id());
        assertThat(admitter.admitNext()).as("only 2 were left").isEqualTo(2);
        assertThat(holdFor("buyer-12", saleItemId, 1).holdId()).isPositive();
    }

    @Test
    void fourAdmittersAtOnceStillLetInOnlyOneSecondsWorth() {
        long saleId = queuedSale(5, 100).id();
        IntStream.rangeClosed(1, 50).forEach(i -> waitingRoom.join(saleId, "buyer-" + i));

        // four app instances' admitters, at the same moment
        List<Concurrently.Outcome<Integer>> admitters = Concurrently.run(4, i -> admitter.admitNext());

        assertThat(admitters.stream().mapToInt(Concurrently.Outcome::value).sum()).isEqualTo(5);
        assertThat(redis.opsForZSet().zCard(WaitingRoom.queueKey(saleId))).isEqualTo(45);
    }

    @Test
    void aPassRunsOut() {
        SaleView sale = queuedSale(5, 10);
        long saleItemId = sale.items().getFirst().saleItemId();
        waitingRoom.join(sale.id(), "alice");
        admitter.admitNext();
        assertThat(holdFor("alice", saleItemId, 1).holdId()).isPositive();

        redis.delete(WaitingRoom.passKey(sale.id(), "alice"));   // what Redis does when the 5 minutes are up

        assertThatThrownBy(() -> holdFor("alice", saleItemId, 1)).isInstanceOf(NotAdmittedException.class);
        assertThat(waitingRoom.status(sale.id(), "alice").state()).isEqualTo(QueueStatus.State.NOT_QUEUED);
    }

    @Test
    void theLineFormsBeforeTheStartAndMovesOnlyOnceTheSaleIsLive() {
        Instant now = clock.instant();
        long saleId = saleService.createSale(new CreateSaleRequest("At 10:00", now.plus(Duration.ofMinutes(10)),
                now.plus(Duration.ofHours(1)), List.of(iphone(10)), null, 5)).id();

        assertThat(waitingRoom.join(saleId, "early-bird").position()).isEqualTo(1);
        assertThat(admitter.admitNext()).as("not live yet: nobody goes in").isZero();

        clock.advance(Duration.ofMinutes(10));
        assertThat(admitter.admitNext()).isEqualTo(1);
        assertThat(waitingRoom.status(saleId, "early-bird").state()).isEqualTo(QueueStatus.State.ADMITTED);
    }

    @Test
    void anUnknownOrFinishedSaleHasNoLine() {
        long saleId = queuedSale(5, 10).id();
        clock.advance(Duration.ofHours(2));          // the test sale runs for an hour

        assertThatThrownBy(() -> waitingRoom.join(saleId, "late")).isInstanceOf(SaleNotActiveException.class);
        assertThatThrownBy(() -> waitingRoom.join(999, "lost")).isInstanceOf(NotFoundException.class);
    }

    @Test
    void overHttpTheQueueAndTheRefusal() throws Exception {
        SaleView sale = queuedSale(5, 10);
        long saleItemId = sale.items().getFirst().saleItemId();

        mvc.perform(post("/api/sales/{saleId}/queue", sale.id()).header("X-User-Id", "alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("WAITING"))
                .andExpect(jsonPath("$.position").value(1))
                .andExpect(jsonPath("$.estimatedWaitSeconds").value(1));
        mvc.perform(post("/api/holds")
                        .header("X-User-Id", "alice")
                        .header("Idempotency-Key", "alice-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"items\": [{\"saleItemId\": %d, \"quantity\": 1}]}".formatted(saleItemId)))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("NOT_ADMITTED"));

        admitter.admitNext();
        mvc.perform(get("/api/sales/{saleId}/queue", sale.id()).header("X-User-Id", "alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("ADMITTED"))
                .andExpect(jsonPath("$.position").doesNotExist());
        mvc.perform(get("/api/sales/{saleId}", sale.id()))
                .andExpect(jsonPath("$.admitPerSecond").value(5));
    }

    /** A live sale with one item, a waiting room letting in {@code perSecond} buyers a second. */
    private SaleView queuedSale(int perSecond, int stock) {
        Instant now = clock.instant();
        return saleService.createSale(new CreateSaleRequest("Queued Sale", now.minus(Duration.ofMinutes(1)),
                now.plus(Duration.ofHours(1)), List.of(iphone(stock)), null, perSecond));
    }

    /** Moves the admission bucket's timestamp back by a second: the next admit sees a second's worth of credit. */
    private void oneSecondLater(long saleId) {
        redis.opsForHash().increment(WaitingRoom.admitKey(saleId), "at", -1000);
    }

    private long saleIdOf(long saleItemId) {
        return jdbc.queryForObject("SELECT sale_id FROM sale_items WHERE id = ?", Long.class, saleItemId);
    }
}
