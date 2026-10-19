package com.flashsale.inventory;

import java.util.Collection;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

public class StockBusyException extends ApiException {

    public StockBusyException(long saleItemId) {
        this("Sale item " + saleItemId + " is very busy right now, please retry");
    }

    /** For a whole cart, e.g. OPTIMISTIC ran out of retries. One line keeps the single-item wording. */
    public StockBusyException(Collection<StockLine> lines) {
        this(describe(lines));
    }

    private StockBusyException(String message) {
        super(HttpStatus.SERVICE_UNAVAILABLE, "STOCK_BUSY", message);
    }

    private static String describe(Collection<StockLine> lines) {
        if (lines.size() == 1) {
            return "Sale item " + lines.iterator().next().saleItemId() + " is very busy right now, please retry";
        }
        return "Sale items " + lines.stream().map(StockLine::saleItemId).sorted().toList()
                + " are very busy right now, please retry";
    }

    @Override
    public Integer retryAfterSeconds() {
        return 1;
    }
}
