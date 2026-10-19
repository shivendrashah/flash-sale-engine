-- V2: the first schema change after V1. Flyway runs it once, at startup, on every database that is still at V1.
-- (V1 is never edited: it has already run, and Flyway would refuse to start because its checksum changed.
--  Every later change gets a new file: V3__..., V4__....)
--
-- The payment recovery job (PaymentRecoveryService) asks every 30 seconds:
--     SELECT ... FROM payments WHERE status = 'PENDING' AND updated_at <= ? ORDER BY id
-- A PARTIAL index contains only the PENDING rows, so it stays tiny even when millions of finished payments pile up,
-- and that question never has to scan the whole table.
CREATE INDEX payments_pending_by_age ON payments (updated_at) WHERE status = 'PENDING';
