package com.flashsale.waitingroom;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Where a buyer stands in a sale's waiting room:
 *   {"saleId":5,"state":"WAITING","position":120,"estimatedWaitSeconds":1}
 *   {"saleId":5,"state":"ADMITTED"}       you may buy now (or the sale has no waiting room)
 *   {"saleId":5,"state":"NOT_QUEUED"}     you haven't joined (only from GET)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueueStatus(long saleId, State state, Long position, Long estimatedWaitSeconds) {

    public enum State {
        /** In line. */
        WAITING,
        /** Has a pass: may create holds until it expires. */
        ADMITTED,
        /** Not in line and no pass. */
        NOT_QUEUED
    }

    static QueueStatus waiting(long saleId, long position, long estimatedWaitSeconds) {
        return new QueueStatus(saleId, State.WAITING, position, estimatedWaitSeconds);
    }

    static QueueStatus admitted(long saleId) {
        return new QueueStatus(saleId, State.ADMITTED, null, null);
    }

    static QueueStatus notQueued(long saleId) {
        return new QueueStatus(saleId, State.NOT_QUEUED, null, null);
    }
}
