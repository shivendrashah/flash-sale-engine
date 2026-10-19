package com.flashsale.waitingroom;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs the waiting room's admitter every 250 ms.
 *
 *       which is smoother for the checkout behind it (HLD.md 9.15).
 */
@Component
@ConditionalOnProperty(prefix = "flashsale.waiting-room", name = "admitter-enabled", havingValue = "true",
        matchIfMissing = true)
class WaitingRoomScheduler {

    private final WaitingRoomAdmitter admitter;

    WaitingRoomScheduler(WaitingRoomAdmitter admitter) {
        this.admitter = admitter;
    }

    @Scheduled(fixedDelayString = "${flashsale.waiting-room.admit-interval}")
    void admitNext() {
        admitter.admitNext();
    }
}
