package com.flashsale.demo;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.flashsale.catalog.SaleNotActiveException;
import com.flashsale.catalog.SaleService;
import com.flashsale.catalog.SaleView;
import com.flashsale.catalog.SoldOutException;
import com.flashsale.common.Pause;
import com.flashsale.common.error.ApiException;
import com.flashsale.hold.CreateHoldCommand;
import com.flashsale.hold.HoldService;
import com.flashsale.hold.HoldView;
import com.flashsale.inventory.ReservationMode;
import com.flashsale.inventory.StockBusyException;
import com.flashsale.payment.PayCommand;
import com.flashsale.payment.PaymentProvider;
import com.flashsale.payment.PaymentService;
import com.flashsale.payment.PaymentView;
import com.flashsale.waitingroom.NotAdmittedException;
import com.flashsale.waitingroom.QueueStatus;
import com.flashsale.waitingroom.WaitingRoomService;

/**
 * Sends a crowd of fake buyers at a sale, so someone watching the demo page sees a flash sale happen: the stock
 * drops, the line moves, orders appear, and "oversold" stays 0. Each fake buyer does what a person does:
 *   1. arrives (spread evenly over the chosen seconds; not before the sale starts, unless there is a line)
 *   2. if the sale has a waiting room: joins the line and waits for a pass (gives up after queue-patience)
 *   3. holds 1 unit
 *   4. thinks for a moment, then pays with a test card that works. Every 7th buyer changes their mind and
 *      cancels instead, so units go back on sale and later buyers get them.
 * Each buyer is one virtual thread. Its outcome is counted in a Redis hash, which every app instance can read.
 */
@Component
public class CrowdSimulator {

    private static final Logger log = LoggerFactory.getLogger(CrowdSimulator.class);
    private static final Duration KEEP_COUNTERS = Duration.ofHours(1);
    private static final Duration POLL_THE_LINE = Duration.ofMillis(500);

    /** The counters, in the order the demo page shows them. */
    static final List<String> COUNTERS = List.of("sent", "arrived", "held", "paid", "cancelled", "soldOut", "busy",
            "notAdmitted", "failed", "done");

    private final SaleService saleService;
    private final HoldService holdService;
    private final PaymentService paymentService;
    private final WaitingRoomService waitingRoom;
    private final StringRedisTemplate redis;
    private final DemoProperties properties;
    private final Clock clock;
    private final ExecutorService buyers = Executors.newVirtualThreadPerTaskExecutor();

    public CrowdSimulator(SaleService saleService, HoldService holdService, PaymentService paymentService,
                          WaitingRoomService waitingRoom, StringRedisTemplate redis, DemoProperties properties,
                          Clock clock) {
        this.saleService = saleService;
        this.holdService = holdService;
        this.paymentService = paymentService;
        this.waitingRoom = waitingRoom;
        this.redis = redis;
        this.properties = properties;
        this.clock = clock;
    }

    public static String countersKey(long saleId) {
        return "demo:crowd:" + saleId;
    }

    /**
     * @throws SaleNotActiveException if the sale has ended
     * @throws CrowdTooEarlyException if it starts later than flashsale.demo.max-lead-time
     */
    public void send(long saleId, CrowdRequest request) {
        SaleView sale = saleService.getSale(saleId);   // 404 if there is no such sale
        Instant now = clock.instant();
        if (!now.isBefore(sale.endsAt())) {
            throw new SaleNotActiveException(saleId);
        }
        if (sale.startsAt().isAfter(now.plus(properties.maxLeadTime()))) {
            throw new CrowdTooEarlyException(saleId, properties.maxLeadTime().toMinutes());
        }
        boolean line = sale.admitPerSecond() != null;
        Duration untilOpen = sale.startsAt().isAfter(now) ? Duration.between(now, sale.startsAt()) : Duration.ZERO;
        long opensAtNanos = System.nanoTime() + untilOpen.toNanos();
        // Without a line, the crowd arrives when the sale opens. With one, it arrives now and queues before the start,
        // the way a real line forms outside a shop.
        Duration firstArrival = line ? Duration.ZERO : untilOpen;
        long gapMillis = request.seconds() * 1000L / request.buyers();
        String run = UUID.randomUUID().toString().substring(0, 8);   // fresh buyer ids for every crowd
        AtomicInteger left = new AtomicInteger(request.buyers());
        count(saleId, "sent", request.buyers());
        log.info("Demo crowd {}: {} buyers for sale {} over {} s{}", run, request.buyers(), saleId, request.seconds(),
                line ? ", through the waiting room" : "");
        for (int i = 0; i < request.buyers(); i++) {
            Duration arrival = firstArrival.plusMillis(i * gapMillis);
            SaleView.Item item = sale.items().get(i % sale.items().size());
            boolean changesMind = i % 7 == 6;
            String buyer = "crowd-" + run + "-" + i;
            buyers.submit(() -> {
                try {
                    buy(sale, item, buyer, arrival, opensAtNanos, changesMind);
                } finally {
                    count(saleId, "done", 1);
                    if (left.decrementAndGet() == 0) {
                        log.info("Demo crowd {} for sale {} finished: {}", run, saleId, progress(saleId));
                    }
                }
            });
        }
    }

    /** One fake buyer, start to end. Every outcome is counted once. */
    private void buy(SaleView sale, SaleView.Item item, String buyer, Duration arrival, long opensAtNanos,
                     boolean changesMind) {
        long saleId = sale.id();
        try {
            Pause.of(arrival);
            count(saleId, "arrived", 1);
            if (sale.admitPerSecond() != null && !waitForPass(sale, buyer, opensAtNanos)) {
                count(saleId, "notAdmitted", 1);
                return;
            }
            HoldView hold = holdService.createHold(new CreateHoldCommand(buyer, "hold-" + buyer, null, null,
                    ReservationMode.ALL_OR_NOTHING, List.of(new CreateHoldCommand.Item(item.saleItemId(), 1)))).body();
            count(saleId, "held", 1);
            Pause.of(randomUpTo(properties.thinkTime()));
            if (changesMind) {
                holdService.cancelHold(buyer, hold.holdId());   // the unit goes back on sale
                count(saleId, "cancelled", 1);
                return;
            }
            PaymentView payment = paymentService.pay(new PayCommand(buyer, hold.holdId(), "pay-" + buyer,
                    PaymentProvider.STRIPE, "tok_success")).body();
            count(saleId, payment.orderId() != null ? "paid" : "failed", 1);
        } catch (SoldOutException e) {
            count(saleId, "soldOut", 1);      // a correct "no": the expected end for most of a flash-sale crowd
        } catch (StockBusyException e) {
            count(saleId, "busy", 1);         // "try again": a real buyer would click again
        } catch (NotAdmittedException e) {
            count(saleId, "notAdmitted", 1);  // the pass ran out between the line and the hold
        } catch (ApiException e) {
            count(saleId, "failed", 1);       // e.g. the sale ended while the buyer waited
            log.debug("Demo buyer {} stopped: {} {}", buyer, e.code(), e.getMessage());
        } catch (RuntimeException e) {
            count(saleId, "failed", 1);
            log.warn("Demo buyer {} failed", buyer, e);
        }
    }

    /** Joins the line, then asks every 0.5 s until let in. False = gave up (queue-patience). */
    private boolean waitForPass(SaleView sale, String buyer, long opensAtNanos) {
        waitingRoom.join(sale.id(), buyer);
        // patience starts when the sale does: waiting before the start is part of the show
        long giveUpAt = Math.max(System.nanoTime(), opensAtNanos) + properties.queuePatience().toNanos();
        while (System.nanoTime() - giveUpAt < 0) {
            if (waitingRoom.status(sale.id(), buyer).state() == QueueStatus.State.ADMITTED) {
                return true;
            }
            Pause.of(POLL_THE_LINE);
        }
        return false;
    }

    /** The crowd's counters for this sale (all crowds sent to it in the last hour), in COUNTERS order. */
    public Map<String, Long> progress(long saleId) {
        Map<String, Long> progress = new LinkedHashMap<>();
        try {
            Map<Object, Object> hash = redis.opsForHash().entries(countersKey(saleId));
            if (!hash.isEmpty()) {
                COUNTERS.forEach(name -> progress.put(name, Long.parseLong((String) hash.getOrDefault(name, "0"))));
            }
        } catch (RuntimeException e) {
            log.debug("Could not read the demo counters of sale {}: {}", saleId, e.toString());   // Redis down
        }
        return progress;
    }

    private void count(long saleId, String counter, long by) {
        try {
            redis.opsForHash().increment(countersKey(saleId), counter, by);
            redis.expire(countersKey(saleId), KEEP_COUNTERS);
        } catch (RuntimeException e) {
            log.debug("Could not count {} for sale {}: {}", counter, saleId, e.toString());   // the demo goes on
        }
    }

    private static Duration randomUpTo(Duration max) {
        return max.isZero() ? Duration.ZERO : Duration.ofMillis(ThreadLocalRandom.current().nextLong(max.toMillis() + 1));
    }

    /** On shutdown: stop the buyers still waiting (interrupts them), so the app can exit. */
    @PreDestroy
    void stop() {
        buyers.shutdownNow();
    }
}
