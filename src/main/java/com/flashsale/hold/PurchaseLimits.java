package com.flashsale.hold;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.flashsale.catalog.Sale;

/**
 * A sale may cap how many units one buyer can hold or buy in it (sales.per_user_limit). This class keeps a
 * running total per (sale, buyer) in the user_sale_units table and enforces the cap ATOMICALLY: two tabs of
 * the same buyer, sending requests at the same moment, can't both slip under it.
 *   refuseIfNoRoom()  before Redis or any stock row is touched: a cheap early "no"
 *   claim()           inside the hold's transaction: add the hold's units, or refuse (409)
 *   giveBack()        inside the expiry or cancel transaction: an ended hold's units come off
 * HELD, PAYMENT_PENDING and CONFIRMED holds count; an EXPIRED or CANCELLED hold gives its units back.
 */
@Component
public class PurchaseLimits {

    /** Buyers in a fixed order (sale id, then user id): every transaction updates their rows in this order. */
    private static final Comparator<BuyerInSale> BY_SALE_THEN_USER =
            Comparator.comparingLong(BuyerInSale::saleId).thenComparing(BuyerInSale::userId);

    private static final String CLAIM = """
            INSERT INTO user_sale_units (sale_id, user_id, units, max_units)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (sale_id, user_id) DO UPDATE
                SET units = user_sale_units.units + EXCLUDED.units
              WHERE user_sale_units.units + EXCLUDED.units <= user_sale_units.max_units
            """;

    private final JdbcTemplate jdbc;

    public PurchaseLimits(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Units this buyer holds or bought in this sale right now (0 if none were ever counted). */
    public int unitsHeld(long saleId, String userId) {
        List<Integer> units = jdbc.queryForList(
                "select units from user_sale_units where sale_id = ? and user_id = ?", Integer.class, saleId, userId);
        return units.isEmpty() ? 0 : units.getFirst();
    }

    /**
     * the buyer's units plus the fewest units this request can end up holding would pass the limit. A plain read, so
     * only an early "no": claim() makes the real decision, atomically.
     */
    public void refuseIfNoRoom(Sale sale, String userId, int fewestUnits) {
        Integer limit = sale.getPerUserLimit();
        if (limit == null) {
            return; // this sale has no limit
        }
        int held = unitsHeld(sale.getId(), userId);
        if (held + fewestUnits > limit) {
            throw new PurchaseLimitExceededException(sale.getId(), limit, held, fewestUnits);
        }
    }

    /**
     * they don't fit, and the caller's ROLLBACK then gives back the stock taken. Two holds of the same buyer racing:
     * the second one waits for the first to commit, then sees its units.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void claim(Sale sale, String userId, int units) {
        Integer limit = sale.getPerUserLimit();
        if (limit == null) {
            return; // no limit: nothing is counted for this sale
        }
        if (units > limit) {
            // More than the whole limit in one hold. (Checked here because a first INSERT of it would break the
            // table's CHECK constraint: an error instead of a clean "no".)
            throw new PurchaseLimitExceededException(sale.getId(), limit, unitsHeld(sale.getId(), userId), units);
        }
        int changed = jdbc.update(CLAIM, sale.getId(), userId, units, limit);
        if (changed == 0) {
            throw new PurchaseLimitExceededException(sale.getId(), limit, unitsHeld(sale.getId(), userId), units);
        }
    }

    /**
     * Call it AFTER StockReleaser.release: every transaction locks stock rows before buyer rows.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void giveBack(Collection<Hold> ended) {
        Map<BuyerInSale, Integer> unitsByBuyer = new TreeMap<>(BY_SALE_THEN_USER);
        for (Hold hold : ended) {
            unitsByBuyer.merge(new BuyerInSale(hold.getSaleId(), hold.getUserId()), hold.totalUnits(), Integer::sum);
        }
        unitsByBuyer.forEach((buyer, units) -> jdbc.update(
                "update user_sale_units set units = units - ? where sale_id = ? and user_id = ?",
                units, buyer.saleId(), buyer.userId()));
    }

    /** One buyer in one sale: the key of a user_sale_units row. */
    private record BuyerInSale(long saleId, String userId) {
    }
}
