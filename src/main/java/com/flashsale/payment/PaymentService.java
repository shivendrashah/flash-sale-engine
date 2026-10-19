package com.flashsale.payment;

import java.time.Clock;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.MeterRegistry;

import com.flashsale.common.IdempotentResult;
import com.flashsale.common.error.IdempotencyKeyReusedException;
import com.flashsale.common.error.NotFoundException;
import com.flashsale.hold.Hold;
import com.flashsale.hold.HoldRepository;
import com.flashsale.hold.HoldStatus;
import com.flashsale.hold.PaymentInProgressException;
import com.flashsale.order.Order;
import com.flashsale.order.OrderConfirmedEvent;
import com.flashsale.order.OrderRepository;
import com.flashsale.outbox.Outbox;
import com.flashsale.payment.gateway.ChargeRequest;
import com.flashsale.payment.gateway.ChargeResult;
import com.flashsale.payment.gateway.GatewayTimeoutException;
import com.flashsale.payment.gateway.GatewayUnavailableException;
import com.flashsale.payment.gateway.PaymentGatewayFactory;

/**
 * Pays for a hold.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final HoldRepository holds;
    private final PaymentRepository payments;
    private final OrderRepository orders;
    private final Outbox outbox;
    private final PaymentGatewayFactory gateways;
    private final TransactionTemplate tx;
    private final PaymentProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;

    public PaymentService(HoldRepository holds, PaymentRepository payments, OrderRepository orders, Outbox outbox,
                          PaymentGatewayFactory gateways, TransactionTemplate tx, PaymentProperties properties,
                          MeterRegistry meters, Clock clock) {
        this.holds = holds;
        this.payments = payments;
        this.orders = orders;
        this.outbox = outbox;
        this.gateways = gateways;
        this.tx = tx;
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
    }

    public IdempotentResult<PaymentView> pay(PayCommand command) {
        String fingerprint = command.fingerprint();

        Optional<PaymentView> earlier = findEarlierResult(command, fingerprint);
        if (earlier.isPresent()) {
            return IdempotentResult.replayed(earlier.get());
        }

        Payment pending;
        try {
            pending = tx.execute(status -> {
                Hold hold = holds.lockOwnedBy(command.userId(), command.holdId());
                if (payments.findByUserIdAndIdempotencyKey(command.userId(), command.idempotencyKey()).isPresent()) {
                    return null; // answer like that copy did (see below)
                }
                hold.startPayment(clock.instant());                                 // HELD -> PAYMENT_PENDING
                return payments.saveAndFlush(Payment.start(hold.getId(), command.userId(), command.provider(),
                        hold.getTotalAmount(), command.idempotencyKey(), fingerprint, clock.instant()));
            });
        } catch (DataIntegrityViolationException duplicate) {
            // The same request arrived twice at once and the other copy saved first (UNIQUE constraint).
            return findEarlierResult(command, fingerprint)
                    .map(IdempotentResult::replayed)
                    .orElseThrow(() -> duplicate);
        }
        if (pending == null) {
            // A copy of this request got the lock first: replay its result, or 409 PAYMENT_IN_PROGRESS if it is
            // still talking to the gateway (findEarlierResult decides).
            return IdempotentResult.replayed(findEarlierResult(command, fingerprint).orElseThrow());
        }

        ChargeRequest request = new ChargeRequest(pending.getGatewayKey(), pending.getAmount(), command.paymentToken());
        ChargeResult charge;
        try {
            charge = gateways.get(command.provider()).charge(request);
        } catch (GatewayUnavailableException nothingCharged) {
            tx.executeWithoutResult(status -> undoAttempt(pending.getId(), command.holdId()));
            throw new PaymentProviderUnavailableException(command.provider(), properties.retryAfterSeconds());
        } catch (GatewayTimeoutException maybeCharged) {
            log.warn("Payment {}: {}. Left PENDING for the recovery job", pending.getId(), maybeCharged.getMessage());
            return IdempotentResult.created(toView(pending, HoldStatus.PAYMENT_PENDING, null));
        }

        PaymentView result = tx.execute(status -> recordOutcome(pending.getId(), command.holdId(), charge));
        return IdempotentResult.created(result);
    }

    /**
     * creates the order and puts an ORDER_CONFIRMED event in the outbox; a decline puts the hold back to HELD.
     * Used by pay() and by PaymentRecoveryService.
     * <p>
     * Safe to call twice: if the payment is no longer PENDING, someone else (the recovery job, or the request it was
     * waiting for) finished it already, and nothing changes.
     */
    PaymentView recordOutcome(long paymentId, long holdId, ChargeResult charge) {
        Hold hold = holds.findByIdForUpdate(holdId).orElseThrow(() -> new NotFoundException("Hold", holdId));
        Payment payment = payments.findById(paymentId).orElseThrow(() -> new NotFoundException("Payment", paymentId));
        if (payment.getStatus() != PaymentStatus.PENDING) {
            if (charge.succeeded() && payment.getStatus() == PaymentStatus.FAILED) {
                // The provider says "charged", but we already gave up on this payment (the recovery job found no
                // trace of it, then a stalled request's charge went through after all). Money was taken for a failed
                // payment: it must be refunded. Refunds are design-only in this project (PLAN.md), so: alarm.
                log.error("Payment {} was charged ({}) after it had been marked FAILED: refund needed",
                        payment.getId(), charge.providerReference());
                meters.counter("flashsale.payments.needs_refund").increment();
            }
            return toView(payment, hold.getStatus(), orderIdOf(payment));
        }
        if (charge.succeeded()) {
            payment.succeeded(charge.providerReference(), clock.instant());
            hold.confirm(clock.instant());                                     // PAYMENT_PENDING -> CONFIRMED
            Order order = orders.save(Order.place(hold.getId(), payment.getId(), hold.getUserId(),
                    hold.getTotalAmount(), clock.instant()));
            outbox.add(OrderConfirmedEvent.of(order));
            return toView(payment, hold.getStatus(), order.getId());
        }
        payment.failed(charge.failureReason(), clock.instant());
        hold.paymentDeclined(clock.instant());                                 // PAYMENT_PENDING -> HELD
        return toView(payment, hold.getStatus(), null);
    }

    /** Ending 3: nothing was charged, so undo transaction 1: forget the attempt, free the hold. */
    private void undoAttempt(long paymentId, long holdId) {
        Hold hold = holds.findByIdForUpdate(holdId).orElseThrow(() -> new NotFoundException("Hold", holdId));
        payments.findById(paymentId)
                .filter(payment -> payment.getStatus() == PaymentStatus.PENDING)
                .ifPresent(payment -> {
                    payments.delete(payment);
                    hold.paymentDeclined(clock.instant());     // PAYMENT_PENDING -> HELD: the user may try again
                });
    }

    private Optional<PaymentView> findEarlierResult(PayCommand command, String fingerprint) {
        return payments.findByUserIdAndIdempotencyKey(command.userId(), command.idempotencyKey())
                .map(payment -> {
                    if (!payment.getRequestHash().equals(fingerprint)) {
                        throw new IdempotencyKeyReusedException(command.idempotencyKey());
                    }
                    if (payment.getStatus() == PaymentStatus.PENDING) {
                        // the first copy is still running, or waiting for the recovery job: 409, retry later
                        throw new PaymentInProgressException(payment.getHoldId());
                    }
                    Hold hold = holds.findById(payment.getHoldId())
                            .orElseThrow(() -> new NotFoundException("Hold", payment.getHoldId()));
                    return toView(payment, hold.getStatus(), orderIdOf(payment));
                });
    }

    private Long orderIdOf(Payment payment) {
        return payment.getStatus() == PaymentStatus.SUCCEEDED
                ? orders.findByHoldId(payment.getHoldId()).map(Order::getId).orElse(null)
                : null;
    }

    private static PaymentView toView(Payment payment, HoldStatus holdStatus, Long orderId) {
        return new PaymentView(payment.getId(), payment.getHoldId(), payment.getStatus(), holdStatus,
                payment.getProvider(), payment.getAmount(), payment.getProviderRef(), payment.getFailureReason(),
                orderId);
    }
}
