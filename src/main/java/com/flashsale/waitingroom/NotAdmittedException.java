package com.flashsale.waitingroom;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

/**
 * The sale has a waiting room and this buyer has no pass: never joined, still in line, or the pass ran out.
 */
public class NotAdmittedException extends ApiException {

    public NotAdmittedException(long saleId) {
        super(HttpStatus.FORBIDDEN, "NOT_ADMITTED", "Sale " + saleId + " has a waiting room: join it with POST "
                + "/api/sales/" + saleId + "/queue and buy once your status is ADMITTED");
    }
}
