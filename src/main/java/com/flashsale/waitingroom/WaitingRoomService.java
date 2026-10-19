package com.flashsale.waitingroom;

import java.time.Clock;

import org.springframework.stereotype.Service;

import com.flashsale.catalog.Sale;
import com.flashsale.catalog.SaleNotActiveException;
import com.flashsale.catalog.SaleRepository;
import com.flashsale.common.error.NotFoundException;

/**
 * Joining a sale's waiting room and asking for one's place. Loads the sale (404 if unknown; 409 once it has
 * ended), then asks WaitingRoom. A buyer may join before the sale starts: the line forms early, and the
 * admitter starts letting people in at the start time.
 */
@Service
public class WaitingRoomService {

    private final SaleRepository sales;
    private final WaitingRoom waitingRoom;
    private final Clock clock;

    public WaitingRoomService(SaleRepository sales, WaitingRoom waitingRoom, Clock clock) {
        this.sales = sales;
        this.waitingRoom = waitingRoom;
        this.clock = clock;
    }

    /** Joins the line (safe to repeat: the buyer keeps their place). */
    public QueueStatus join(long saleId, String userId) {
        return waitingRoom.join(openSale(saleId), userId);
    }

    public QueueStatus status(long saleId, String userId) {
        return waitingRoom.status(openSale(saleId), userId);
    }

    /** The sale, if it hasn't ended yet (joining the line of a finished sale makes no sense). */
    private Sale openSale(long saleId) {
        Sale sale = sales.findById(saleId).orElseThrow(() -> new NotFoundException("Sale", saleId));
        if (!clock.instant().isBefore(sale.getEndsAt())) {
            throw new SaleNotActiveException(saleId);
        }
        return sale;
    }
}
