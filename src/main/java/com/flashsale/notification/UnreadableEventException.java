package com.flashsale.notification;

public class UnreadableEventException extends RuntimeException {

    public UnreadableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
