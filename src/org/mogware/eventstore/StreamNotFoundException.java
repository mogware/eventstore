package org.mogware.eventstore;

public class StreamNotFoundException extends RuntimeException {
    public StreamNotFoundException() {
    }
    public StreamNotFoundException(String message) {
        super(message);
    }
    public StreamNotFoundException(String message, Exception innerException) {
        super(message, innerException);
    }
}
