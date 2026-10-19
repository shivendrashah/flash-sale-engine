package com.flashsale.catalog;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * A sale is a time window during which products are sold at a discount with limited stock,
 * e.g. "Diwali Flash Sale, 10:00 to 10:30".
 */
@Entity
@Table(name = "sales")
public class Sale {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 200)
    private String name;

    @Column(name = "starts_at", nullable = false)
    private Instant startsAt;

    @Column(name = "ends_at", nullable = false)
    private Instant endsAt;

    /** The most units one buyer may hold or buy in this sale; null = no limit (added in the V3 migration). */
    @Column(name = "per_user_limit")
    private Integer perUserLimit;

    /**
     * How many queued buyers the waiting room lets in per second; null = no waiting room (added in V5).
     */
    @Column(name = "admit_per_second")
    private Integer admitPerSecond;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Sale() {
    }

    /** @param perUserLimit the most units one buyer may hold or buy in this sale, or null for no limit */
    public Sale(String name, Instant startsAt, Instant endsAt, Integer perUserLimit, Instant createdAt) {
        this(name, startsAt, endsAt, perUserLimit, null, createdAt);
    }

    /** @param admitPerSecond buyers the waiting room lets in per second, or null for no waiting room */
    public Sale(String name, Instant startsAt, Instant endsAt, Integer perUserLimit, Integer admitPerSecond,
                Instant createdAt) {
        this.name = name;
        this.startsAt = startsAt;
        this.endsAt = endsAt;
        this.perUserLimit = perUserLimit;
        this.admitPerSecond = admitPerSecond;
        this.createdAt = createdAt;
    }

    /**
     * True from the start time (inclusive) until the end time (exclusive).
     */
    public boolean isActiveAt(Instant now) {
        return isActiveAt(startsAt, endsAt, now);
    }

    /**
     * The same rule for a sale that is not an entity here: SaleCache works "active" out again on every read of a
     * cached sale page (SaleView.at), so a page cached one second before the start never says "not live yet" late.
     */
    static boolean isActiveAt(Instant startsAt, Instant endsAt, Instant now) {
        return !now.isBefore(startsAt) && now.isBefore(endsAt);
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Instant getStartsAt() {
        return startsAt;
    }

    public Instant getEndsAt() {
        return endsAt;
    }

    /** null = no limit. */
    public Integer getPerUserLimit() {
        return perUserLimit;
    }

    /** null = no waiting room. */
    public Integer getAdmitPerSecond() {
        return admitPerSecond;
    }

    public boolean hasWaitingRoom() {
        return admitPerSecond != null;
    }
}
