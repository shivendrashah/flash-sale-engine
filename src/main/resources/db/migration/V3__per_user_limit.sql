-- V3: the per-user purchase limit (Phase 2, step 4). Flyway runs it once on every database that is still at V2.
--
-- 1. A sale may cap how many units one buyer can hold or buy in it. NULL = no limit.
--    Adding a column that allows NULL and has no default only changes the table's description in PostgreSQL:
--    no rows are rewritten, so it is instant even on a big table.
ALTER TABLE sales
    ADD COLUMN per_user_limit INT CONSTRAINT sales_per_user_limit_positive CHECK (per_user_limit > 0);

-- 2. How many units each buyer holds or bought in each LIMITED sale: one row per (sale, buyer).
--    It is a "linking table" between sales and buyers: its primary key is the pair (a composite key), and it holds
--    data about the pair. HELD, PAYMENT_PENDING and CONFIRMED holds count; an expired or cancelled hold gives its
--    units back. PurchaseLimits (hold package) keeps it up to date in the same transactions that change the holds.
--
--    max_units is a copy of the sale's limit, so the CHECK below can keep every row in range on its own: even a
--    buggy code path can never store more units than the limit allows (the same "last line of defence" idea as
--    sale_items_stock_in_range in V1).
CREATE TABLE user_sale_units (
    sale_id    BIGINT      NOT NULL REFERENCES sales (id),
    user_id    VARCHAR(64) NOT NULL,
    units      INT         NOT NULL,
    max_units  INT         NOT NULL,
    CONSTRAINT user_sale_units_pk PRIMARY KEY (sale_id, user_id),
    CONSTRAINT user_sale_units_in_range CHECK (units BETWEEN 0 AND max_units)
);
