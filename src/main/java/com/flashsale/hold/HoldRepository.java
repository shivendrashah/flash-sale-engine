package com.flashsale.hold;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import jakarta.persistence.LockModeType;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.flashsale.common.error.NotFoundException;

/**
 * Database access for holds.
 */
public interface HoldRepository extends JpaRepository<Hold, Long> {

    /**
     */
    @EntityGraph(attributePaths = "items")
    Optional<Hold> findWithItemsById(long id);

    @EntityGraph(attributePaths = "items")
    Optional<Hold> findByUserIdAndIdempotencyKey(String userId, String idempotencyKey);

    /** SELECT ... FOR UPDATE on one hold, so only one request at a time can change its status. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select h from Hold h where h.id = :id")
    @Transactional(propagation = Propagation.MANDATORY)
    Optional<Hold> findByIdForUpdate(long id);

    /**
     * Locks the hold and checks it belongs to the user. Someone else's hold looks "not found" (404), so callers
     * can't discover which hold ids exist.
     */
    default Hold lockOwnedBy(String userId, long holdId) {
        return findByIdForUpdate(holdId)
                .filter(hold -> hold.getUserId().equals(userId))
                .orElseThrow(() -> new NotFoundException("Hold", holdId));
    }

    @Query(value = """
            SELECT * FROM holds
             WHERE status = 'HELD' AND expires_at <= :now
             ORDER BY expires_at
             LIMIT :batchSize
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    @Transactional(propagation = Propagation.MANDATORY)
    List<Hold> lockHoldsDueForExpiry(Instant now, int batchSize);
}
