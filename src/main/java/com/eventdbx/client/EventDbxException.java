package com.eventdbx.client;

public class EventDbxException extends RuntimeException {
    public EventDbxException(String message) {
        super(message);
    }

    public EventDbxException(String message, Throwable cause) {
        super(message, cause);
    }
}
