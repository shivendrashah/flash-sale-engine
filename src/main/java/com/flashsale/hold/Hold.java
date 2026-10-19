package com.flashsale.hold;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import com.flashsale.inventory.HeldStock;
import com.flashsale.inventory.StockLine;

/**
 * A temporary reservation: stock is set aside for one user for a few minutes while they pay.
 */
@Entity
@Table(name = "holds")
public class Hold {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "sale_id", nullable = false)
    private Long saleId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private HoldStatus status;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    @Column(name = "idempotency_key", nullable = false, length = 100)
    private String idempotencyKey;

    @Column(name = "request_hash", nullable = false, length = 64)
    private String requestHash;

    /** Safety net: if two transactions ever change the same hold without a lock, the second one fails. */
    @Version
    @Column(nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "hold", cascade = CascadeType.ALL, orphanRemoval = true)
    @OrderBy("saleItemId")
    private List<HoldItem> items = new ArrayList<>();

    protected Hold() {
    }

    public static Hold create(String userId, long saleId, List<HoldItem.Line> lines, Instant now, Duration ttl,
                              String idempotencyKey, String requestHash) {
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("A hold needs at least one item");
        }
        Hold hold = new Hold();
        hold.userId = userId;
        hold.saleId = saleId;
        hold.status = HoldStatus.HELD;
        hold.expiresAt = now.plus(ttl); // e.g. now + 10 minutes (flashsale.holds.ttl)
        hold.idempotencyKey = idempotencyKey;
        hold.requestHash = requestHash;
        hold.createdAt = now;
        hold.updatedAt = now;
        for (HoldItem.Line line : lines) {
            hold.items.add(new HoldItem(hold, line.saleItemId(), line.quantity(), line.unitPrice()));
        }
        hold.totalAmount = hold.items.stream().map(HoldItem::lineTotal).reduce(BigDecimal.ZERO, BigDecimal::add);
        return hold;
    }

    /** HELD -> PAYMENT_PENDING. Refused if the hold has run out of time, even if the expiry job hasn't run yet. */
    public void startPayment(Instant now) {
        if (status == HoldStatus.EXPIRED || (status == HoldStatus.HELD && isPastExpiry(now))) {
            throw new HoldExpiredException(id);
        }
        if (status == HoldStatus.PAYMENT_PENDING) {
            throw new PaymentInProgressException(id);
        }
        moveTo(HoldStatus.PAYMENT_PENDING, now);
    }

    /** PAYMENT_PENDING -> CONFIRMED. */
    public void confirm(Instant now) {
        moveTo(HoldStatus.CONFIRMED, now);
    }

    /** PAYMENT_PENDING -> HELD. The user may try another card until the hold expires. */
    public void paymentDeclined(Instant now) {
        moveTo(HoldStatus.HELD, now);
    }

    /** HELD -> CANCELLED. The caller must give the stock back in the same transaction. */
    public void cancel(Instant now) {
        moveTo(HoldStatus.CANCELLED, now);
    }

    /** HELD -> EXPIRED. The caller must give the stock back in the same transaction. */
    public void expire(Instant now) {
        if (!isPastExpiry(now)) {
            throw new IllegalStateException("Hold " + id + " is not due to expire until " + expiresAt);
        }
        moveTo(HoldStatus.EXPIRED, now);
    }

    public boolean isPastExpiry(Instant now) {
        return !now.isBefore(expiresAt);
    }

    /** The stock this hold has set aside, as lines the inventory package understands. */
    public List<StockLine> stockLines() {
        return items.stream().map(item -> new StockLine(item.getSaleItemId(), item.getQuantity())).toList();
    }

    /** How many units this hold contains, over all its items (what counts against the buyer's purchase limit). */
    public int totalUnits() {
        return items.stream().mapToInt(HoldItem::getQuantity).sum();
    }

    /** The same lines plus the sale they belong to: what StockReleaser needs to give them back. */
    public HeldStock heldStock() {
        return new HeldStock(saleId, stockLines());
    }

    /**
     * The one place where the status changes.
     */
    private void moveTo(HoldStatus next, Instant now) {
        if (!status.canMoveTo(next)) {
            throw new IllegalHoldTransitionException(id, status, next); // becomes HTTP 409
        }
        status = next;
        updatedAt = now;
    }

    public Long getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public Long getSaleId() {
        return saleId;
    }

    public HoldStatus getStatus() {
        return status;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public String getRequestHash() {
        return requestHash;
    }

    /** Read-only view of the list: nobody outside can add or remove items behind the Hold's back. */
    public List<HoldItem> getItems() {
        return Collections.unmodifiableList(items);
    }
}
