-- V5: the waiting room (Phase 3). Flyway runs it once on every database that is still at V4.
--
-- A sale may put buyers in a queue and let in only this many per second (WaitingRoom, in Redis). NULL = no waiting
-- room: anyone can buy as soon as the sale is live, as before. Like per_user_limit in V3, a nullable column with no
-- default only changes the table's description, so adding it is instant even on a big table.
ALTER TABLE sales
    ADD COLUMN admit_per_second INT CONSTRAINT sales_admit_per_second_positive CHECK (admit_per_second > 0);
