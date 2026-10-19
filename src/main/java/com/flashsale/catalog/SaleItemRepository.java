package com.flashsale.catalog;

import java.util.List;
import java.util.Optional;

import jakarta.persistence.LockModeType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * All the database operations on stock. Each concurrency strategy uses different methods from here.
 *
 *      transaction that the caller already started, or Spring throws an error. A row lock lasts only until its
 *      transaction ends, so a lock outside a transaction would protect nothing.
 */
public interface SaleItemRepository extends JpaRepository<SaleItem, Long> {

    List<SaleItem> findBySaleIdOrderById(long saleId);

    /**
     * PESSIMISTIC strategy. Locks the row so other buyers WAIT until we commit.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from SaleItem s where s.id = :id")
    @Transactional(propagation = Propagation.MANDATORY)
    Optional<SaleItem> findByIdForUpdate(long id);

    /**
     * CONDITIONAL_UPDATE strategy. Check and subtract in ONE statement. The database runs it atomically, so nobody
     * can sneak in between the check and the write. Returns how many rows changed: 1 = got the stock, 0 = not enough.
     */
    @Modifying
    @Query("""
            update SaleItem s
               set s.availableStock = s.availableStock - :quantity, s.version = s.version + 1
             where s.id = :id and s.availableStock >= :quantity
            """)
    @Transactional(propagation = Propagation.MANDATORY)
    int takeIfAvailable(long id, int quantity);

    /** Puts stock back (expired or cancelled hold). Adding needs no read first, so one statement is safe. */
    @Modifying
    @Query("""
            update SaleItem s
               set s.availableStock = s.availableStock + :quantity, s.version = s.version + 1
             where s.id = :id
            """)
    @Transactional(propagation = Propagation.MANDATORY)
    int giveBack(long id, int quantity);

    /** NAIVE strategy (demo only): a plain read of the stock number, with no lock. */
    @Query("select s.availableStock from SaleItem s where s.id = :id")
    @Transactional(propagation = Propagation.MANDATORY)
    Optional<Integer> findAvailableStock(long id);

    /** NAIVE strategy (demo only): writes a number computed in Java, ignoring what others wrote meanwhile. */
    @Modifying
    @Query("update SaleItem s set s.availableStock = :value where s.id = :id")
    @Transactional(propagation = Propagation.MANDATORY)
    int overwriteAvailableStock(long id, int value);
}
