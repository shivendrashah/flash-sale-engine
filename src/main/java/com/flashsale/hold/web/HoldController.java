package com.flashsale.hold.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flashsale.common.ApiHeaders;
import com.flashsale.common.IdempotentResult;
import com.flashsale.hold.HoldService;
import com.flashsale.hold.HoldView;
import com.flashsale.inventory.ConcurrencyStrategy;

/**
 * The HTTP entry point for holds: create (POST), read (GET), cancel (DELETE).
 *
 *   curl sends "POST /api/holds" -> embedded Tomcat receives it on port 8080 -> Spring MVC's DispatcherServlet
 *   finds the method mapped to POST /api/holds (this one) -> Jackson turns the JSON body into a CreateHoldRequest
 *   -> the headers are copied into the String parameters -> validation runs (@Valid, @NotBlank, @Size).
 *   Any failure stops here with HTTP 400, and createHold() is never called.
 */
@RestController
@RequestMapping("/api/holds")
class HoldController {

    private final HoldService holdService;

    HoldController(HoldService holdService) {
        this.holdService = holdService;
    }

    /** 201 for a new hold; 200 (with Idempotent-Replayed: true) when the same request is retried. */
    @PostMapping
    ResponseEntity<HoldView> createHold(
            @RequestHeader(ApiHeaders.USER_ID) @NotBlank @Size(max = 64) String userId,
            @RequestHeader(ApiHeaders.IDEMPOTENCY_KEY) @NotBlank @Size(max = 100) String idempotencyKey,
            @RequestHeader(name = ApiHeaders.CONCURRENCY_STRATEGY, required = false) ConcurrencyStrategy strategy,
            @RequestHeader(name = ApiHeaders.STOCK_GATE, required = false) Boolean stockGate,
            @Valid @RequestBody CreateHoldRequest request) {
        IdempotentResult<HoldView> result =
                holdService.createHold(request.toCommand(userId, idempotencyKey, strategy, stockGate));
        return ResponseEntity.status(result.replayed() ? HttpStatus.OK : HttpStatus.CREATED)
                .header(ApiHeaders.IDEMPOTENT_REPLAYED, String.valueOf(result.replayed()))
                .body(result.body());
    }

    @GetMapping("/{holdId}")
    HoldView getHold(@RequestHeader(ApiHeaders.USER_ID) @NotBlank String userId, @PathVariable long holdId) {
        return holdService.getHold(userId, holdId);
    }

    @DeleteMapping("/{holdId}")
    HoldView cancelHold(@RequestHeader(ApiHeaders.USER_ID) @NotBlank String userId, @PathVariable long holdId) {
        return holdService.cancelHold(userId, holdId);
    }
}
