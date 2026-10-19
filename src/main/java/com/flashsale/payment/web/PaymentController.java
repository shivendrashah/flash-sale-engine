package com.flashsale.payment.web;

import java.net.URI;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.flashsale.common.ApiHeaders;
import com.flashsale.common.IdempotentResult;
import com.flashsale.payment.PayCommand;
import com.flashsale.payment.PaymentService;
import com.flashsale.payment.PaymentStatus;
import com.flashsale.payment.PaymentView;

/**
 * POST /api/holds/{holdId}/pay: pays for a hold.
 */
@RestController
class PaymentController {

    private final PaymentService paymentService;

    PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /**
     * 200: finished (paid, or a declined card: paymentStatus FAILED and the hold stays HELD).
     * 202: the provider's answer didn't arrive in time; the payment is PENDING until the recovery job finishes it.
     * 503 + Retry-After: the provider is down and nothing was charged (PaymentProviderUnavailableException).
     */
    @PostMapping("/api/holds/{holdId}/pay")
    ResponseEntity<PaymentView> pay(
            @RequestHeader(ApiHeaders.USER_ID) @NotBlank @Size(max = 64) String userId,
            @RequestHeader(ApiHeaders.IDEMPOTENCY_KEY) @NotBlank @Size(max = 100) String idempotencyKey,
            @PathVariable long holdId,
            @Valid @RequestBody PayRequest request) {
        IdempotentResult<PaymentView> result = paymentService.pay(
                new PayCommand(userId, holdId, idempotencyKey, request.provider(), request.paymentToken()));
        if (result.body().paymentStatus() == PaymentStatus.PENDING) {
            return ResponseEntity.accepted()
                    .location(URI.create("/api/holds/" + holdId))
                    .header(ApiHeaders.IDEMPOTENT_REPLAYED, String.valueOf(result.replayed()))
                    .body(result.body());
        }
        return ResponseEntity.ok()
                .header(ApiHeaders.IDEMPOTENT_REPLAYED, String.valueOf(result.replayed()))
                .body(result.body());
    }
}
