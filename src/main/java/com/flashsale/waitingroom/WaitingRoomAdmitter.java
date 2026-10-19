package com.flashsale.waitingroom;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import com.flashsale.catalog.Sale;
import com.flashsale.catalog.SaleRepository;

/**
 * Lets queued buyers in: for every live sale with a waiting room, one ADMIT script call (WaitingRoom.admit).
 *
 *               per sale in Redis, so two instances together still admit admitPerSecond buyers a second
 *               (WaitingRoomTest checks it with four admitters at once).
 */
@Service
public class WaitingRoomAdmitter {

    private static final Logger log = LoggerFactory.getLogger(WaitingRoomAdmitter.class);

    private final SaleRepository sales;
    private final WaitingRoom waitingRoom;
    private final Clock clock;

    public WaitingRoomAdmitter(SaleRepository sales, WaitingRoom waitingRoom, Clock clock) {
        this.sales = sales;
        this.waitingRoom = waitingRoom;
        this.clock = clock;
    }

    /** @return buyers let in by this run, over all sales */
    public int admitNext() {
        int total = 0;
        for (Sale sale : sales.findLiveWithWaitingRoom(clock.instant())) {
            try {
                total += waitingRoom.admit(sale);
            } catch (DataAccessException redisDown) {
                // Nobody can be let in, but nobody needs to be: requirePass fails open while Redis is away
                log.warn("Waiting room admitter skipped sale {}: Redis unreachable or too slow ({})", sale.getId(),
                        redisDown.getMessage());
                return total;
            }
        }
        return total;
    }
}
