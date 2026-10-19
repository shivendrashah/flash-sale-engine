package com.flashsale.outbox;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Database access for the outbox table.
 */
public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    @Query(value = """
            SELECT * FROM outbox_events
             WHERE status = 'PENDING' AND next_attempt_at <= :now
             ORDER BY id
             LIMIT :batchSize
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    @Transactional(propagation = Propagation.MANDATORY)
    List<OutboxEvent> lockDue(Instant now, int batchSize);

    /** For the flashsale.outbox.pending gauge: "SELECT COUNT(*) ... WHERE status = ?". */
    long countByStatus(OutboxStatus status);

    Optional<OutboxEvent> findByEventId(UUID eventId);

    /** The events about one thing, oldest first (tests, and support: "what did we announce about order 42?"). */
    List<OutboxEvent> findByAggregateTypeAndAggregateIdOrderById(String aggregateType, long aggregateId);
}
