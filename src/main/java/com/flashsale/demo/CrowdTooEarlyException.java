package com.flashsale.demo;

import org.springframework.http.HttpStatus;

import com.flashsale.common.error.ApiException;

/**
 * 409 CROWD_TOO_EARLY: the sale starts too far in the future for a crowd to wait for it.
 */
public class CrowdTooEarlyException extends ApiException {

    public CrowdTooEarlyException(long saleId, long minutes) {
        super(HttpStatus.CONFLICT, "CROWD_TOO_EARLY",
                "Sale " + saleId + " starts in more than " + minutes + " minutes. Send the crowd closer to the start.");
    }
}
