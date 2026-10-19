package com.flashsale.waitingroom.web;

import jakarta.validation.constraints.NotBlank;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.flashsale.common.ApiHeaders;
import com.flashsale.waitingroom.QueueStatus;
import com.flashsale.waitingroom.WaitingRoomService;

/**
 * The waiting room's two endpoints:
 *   POST /api/sales/{saleId}/queue   join the line (or keep your place) and see where you stand
 *   GET  /api/sales/{saleId}/queue   see where you stand; a client polls this until state is ADMITTED
 */
@RestController
@RequestMapping("/api/sales/{saleId}/queue")
class WaitingRoomController {

    private final WaitingRoomService waitingRoom;

    WaitingRoomController(WaitingRoomService waitingRoom) {
        this.waitingRoom = waitingRoom;
    }

    @PostMapping
    QueueStatus join(@RequestHeader(ApiHeaders.USER_ID) @NotBlank String userId, @PathVariable long saleId) {
        return waitingRoom.join(saleId, userId);
    }

    @GetMapping
    QueueStatus status(@RequestHeader(ApiHeaders.USER_ID) @NotBlank String userId, @PathVariable long saleId) {
        return waitingRoom.status(saleId, userId);
    }
}
