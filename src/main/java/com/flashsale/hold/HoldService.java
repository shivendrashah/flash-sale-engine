package com.flashsale.hold;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import com.flashsale.catalog.Sale;
import com.flashsale.catalog.SaleItem;
import com.flashsale.catalog.SaleItemRepository;
import com.flashsale.catalog.SaleNotActiveException;
import com.flashsale.catalog.SaleRepository;
import com.flashsale.catalog.SoldOutException;
import com.flashsale.common.IdempotentResult;
import com.flashsale.common.error.IdempotencyKeyReusedException;
import com.flashsale.common.error.NotFoundException;
import com.flashsale.inventory.ReservationPolicy;
import com.flashsale.inventory.ReservationPolicyFactory;
import com.flashsale.inventory.StockGate;
import com.flashsale.inventory.StockLine;
import com.flashsale.inventory.StockReleaser;
import com.flashsale.inventory.StockReserver;
import com.flashsale.inventory.StockReserverFactory;
import com.flashsale.waitingroom.WaitingRoom;

/**
 * The main use cases for holds: create one (a cart of 1 to 10 items), read one, cancel one.
 */
@Service
public class HoldService {

    private final HoldRepository holds;
    private final SaleItemRepository saleItems;
    private final SaleRepository sales;
    private final StockReserverFactory reservers;
    private final ReservationPolicyFactory policies;
    private final StockReleaser stockReleaser;
    private final StockGate stockGate;
    private final PurchaseLimits purchaseLimits;
    private final WaitingRoom waitingRoom;
    private final TransactionTemplate tx;
    private final HoldProperties properties;
    private final Clock clock;

    public HoldService(HoldRepository holds, SaleItemRepository saleItems, SaleRepository sales,
                       StockReserverFactory reservers, ReservationPolicyFactory policies, StockReleaser stockReleaser,
                       StockGate stockGate, PurchaseLimits purchaseLimits, WaitingRoom waitingRoom,
                       TransactionTemplate tx, HoldProperties properties, Clock clock) {
        this.holds = holds;
        this.saleItems = saleItems;
        this.sales = sales;
        this.reservers = reservers;
        this.policies = policies;
        this.stockReleaser = stockReleaser;
        this.stockGate = stockGate;
        this.purchaseLimits = purchaseLimits;
        this.waitingRoom = waitingRoom;
        this.tx = tx;
        this.properties = properties;
        this.clock = clock;
    }

    /** Called by HoldController.createHold with the request already turned into a command. */
    public IdempotentResult<HoldView> createHold(CreateHoldCommand command) {
        String fingerprint = command.fingerprint();
        List<StockLine> wanted = command.stockLines();

        Optional<HoldView> earlier = findEarlierResult(command, fingerprint, wanted);
        if (earlier.isPresent()) {
            return IdempotentResult.replayed(earlier.get());
        }

        Map<Long, SaleItem> itemsById = loadSaleItems(wanted);
        Sale sale = theOneSaleOf(itemsById.values());
        Instant now = clock.instant();
        if (!sale.isActiveAt(now)) {
            throw new SaleNotActiveException(sale.getId());
        }
        waitingRoom.requirePass(sale, command.userId());

        StockReserver reserver = reservers.get(command.strategy());
        ReservationPolicy policy = policies.get(command.mode());

        try {
            purchaseLimits.refuseIfNoRoom(sale, command.userId(), policy.fewestUnits(wanted));
        } catch (PurchaseLimitExceededException overLimit) {
            return replayOr(command, fingerprint, wanted, overLimit); // a copy of this request may have just won
        }

        StockGate.Admission admission = stockGate.admit(sale.getId(), command.requestId(), wanted, itemsById, policy,
                command.stockGate());
        List<StockLine> keptByDatabase = List.of(); // filled in only if the transaction below commits
        try {
            HoldView created = reserver.reserve(admission.letThrough(), policy, taken -> {
                policy.onFinished(taken, wanted);
                List<HoldItem.Line> lines = taken.stream()
                        .map(line -> new HoldItem.Line(line.saleItemId(), line.quantity(),
                                itemsById.get(line.saleItemId()).getSalePrice()))  // the price is copied now
                        .toList();
                Hold hold = Hold.create(command.userId(), sale.getId(), lines, now, properties.ttl(),
                        command.idempotencyKey(), fingerprint);
                Hold saved = holds.saveAndFlush(hold);
                purchaseLimits.claim(sale, command.userId(), saved.totalUnits());
                // "unavailable" = every cart line not held: refused by Redis, or skipped by the database
                return HoldView.of(saved, notTaken(wanted, taken));
            });
            keptByDatabase = linesOf(created);
            return IdempotentResult.created(created);
        } catch (DataIntegrityViolationException | SoldOutException | PurchaseLimitExceededException lostToACopy) {
            IdempotentResult<HoldView> replay = replayOr(command, fingerprint, wanted, lostToACopy);
            keptByDatabase = linesOf(replay.body()); // units Redis gave this request now belong to that hold
            return replay;
        } finally {
            stockGate.giveBackUnused(admission, keptByDatabase);
        }
    }

    /** GET /api/holds/{id}. Someone else's hold is reported as "not found". */
    public HoldView getHold(String userId, long holdId) {
        return holds.findWithItemsById(holdId)
                .filter(hold -> hold.getUserId().equals(userId))
                .map(HoldView::of)
                .orElseThrow(() -> new NotFoundException("Hold", holdId));
    }

    /** DELETE /api/holds/{id}. */
    public HoldView cancelHold(String userId, long holdId) {
        return tx.execute(status -> {
            Hold hold = holds.lockOwnedBy(userId, holdId);
            hold.cancel(clock.instant());                    // HELD -> CANCELLED (in memory; UPDATE at commit)
            stockReleaser.release(List.of(hold.heldStock()));
            purchaseLimits.giveBack(List.of(hold));         // the buyer's units for this sale, last (lock order)
            return HoldView.of(hold);
        });
    }

    /** Every cart item, by id. 404 for the lowest id that doesn't exist. */
    private Map<Long, SaleItem> loadSaleItems(List<StockLine> wanted) {
        Map<Long, SaleItem> byId = saleItems.findAllById(wanted.stream().map(StockLine::saleItemId).toList())
                .stream()
                .collect(Collectors.toMap(SaleItem::getId, Function.identity()));
        for (StockLine line : StockLine.inIdOrder(wanted)) {
            if (!byId.containsKey(line.saleItemId())) {
                throw new NotFoundException("Sale item", line.saleItemId());
            }
        }
        return byId;
    }

    /** The sale every item belongs to. A hold has one sale_id, so a cart can't mix sales (422). */
    private Sale theOneSaleOf(Collection<SaleItem> items) {
        Set<Long> saleIds = items.stream().map(SaleItem::getSaleId).collect(Collectors.toCollection(TreeSet::new));
        if (saleIds.size() > 1) {
            throw new MixedSalesException(saleIds);
        }
        long saleId = saleIds.iterator().next();
        return sales.findById(saleId).orElseThrow(() -> new NotFoundException("Sale", saleId));
    }

    /** A refusal for a request that a copy of itself already won isn't a refusal: answer with that copy's hold. */
    private IdempotentResult<HoldView> replayOr(CreateHoldCommand command, String fingerprint, List<StockLine> wanted,
                                               RuntimeException refusal) {
        return findEarlierResult(command, fingerprint, wanted)
                .map(IdempotentResult::replayed)
                .orElseThrow(() -> refusal);
    }

    /** The lines a saved hold really contains (what PostgreSQL kept). */
    private static List<StockLine> linesOf(HoldView hold) {
        return hold.items().stream().map(item -> new StockLine(item.saleItemId(), item.quantity())).toList();
    }

    /** The cart lines that were asked for but not held (PARTIAL skipped them). Empty for a complete hold. */
    private static List<StockLine> notTaken(List<StockLine> wanted, List<StockLine> taken) {
        Set<Long> takenIds = taken.stream().map(StockLine::saleItemId).collect(Collectors.toSet());
        return StockLine.inIdOrder(wanted).stream().filter(line -> !takenIds.contains(line.saleItemId())).toList();
    }

    private Optional<HoldView> findEarlierResult(CreateHoldCommand command, String fingerprint,
                                                 List<StockLine> wanted) {
        return holds.findByUserIdAndIdempotencyKey(command.userId(), command.idempotencyKey())
                .map(hold -> {
                    if (!hold.getRequestHash().equals(fingerprint)) {
                        // same key, different request: 422
                        throw new IdempotencyKeyReusedException(command.idempotencyKey());
                    }
                    return HoldView.of(hold, notTaken(wanted, hold.stockLines()));
                });
    }
}
