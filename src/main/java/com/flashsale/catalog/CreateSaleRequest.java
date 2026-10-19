package com.flashsale.catalog;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

/**
 * The JSON body of POST /api/admin/sales: a sale and its products, in one call. Example:
 *   {"name": "Diwali Flash Sale", "startsAt": "2026-01-01T00:00:00Z", "endsAt": "2030-01-01T00:00:00Z",
 *    "items": [{"productName": "iPhone 17", "regularPrice": 79999, "salePrice": 59999, "stock": 100}],
 *    "perUserLimit": 2}
 * "perUserLimit" is optional: without it, a buyer may take any number of units.
 */
public record CreateSaleRequest(
        @NotBlank @Size(max = 200) String name,
        @NotNull Instant startsAt,
        @NotNull Instant endsAt,
        @NotEmpty List<@Valid Item> items,
        @Positive Integer perUserLimit,
        @Positive @Max(100_000) Integer admitPerSecond) {

    /** Without a per-user limit (the Phase 1 shape, still used by most tests). */
    public CreateSaleRequest(String name, Instant startsAt, Instant endsAt, List<Item> items) {
        this(name, startsAt, endsAt, items, null, null);
    }

    /** With a per-user limit, without a waiting room (the Phase 2 shape). */
    public CreateSaleRequest(String name, Instant startsAt, Instant endsAt, List<Item> items, Integer perUserLimit) {
        this(name, startsAt, endsAt, items, perUserLimit, null);
    }

    public record Item(
            @NotBlank @Size(max = 200) String productName,
            @NotNull @PositiveOrZero BigDecimal regularPrice,
            @NotNull @PositiveOrZero BigDecimal salePrice,
            @Min(0) @Max(1_000_000) int stock) {
    }

    @AssertTrue(message = "endsAt must be after startsAt")
    public boolean isWindowValid() {
        return startsAt == null || endsAt == null || endsAt.isAfter(startsAt);
    }
}
