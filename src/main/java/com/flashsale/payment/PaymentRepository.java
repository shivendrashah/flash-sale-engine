package com.flashsale.payment;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/**
 * Database access for payments.
 */
public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findByUserIdAndIdempotencyKey(String userId, String idempotencyKey);

    /**
     * ago, so the provider's answer was lost (a timeout, or the app crashed between B4 and B5).
     */
    @Query("select p from Payment p where p.status = com.flashsale.payment.PaymentStatus.PENDING"
            + " and p.updatedAt <= :cutoff order by p.id")
    List<Payment> findStuck(Instant cutoff, Limit limit);
}
