package org.mogware.eventstore;

public class ConcurrencyException extends RuntimeException {
    public ConcurrencyException() {
    }
    public ConcurrencyException(String message) {
        super(message);
    }
    public ConcurrencyException(String message, Exception innerException) {
        super(message, innerException);
    }
}
