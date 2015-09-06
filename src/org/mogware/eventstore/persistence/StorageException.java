package org.mogware.eventstore.persistence;

public class StorageException extends RuntimeException {
    public StorageException() {
    }
    public StorageException(String message) {
        super(message);
    }
    public StorageException(String message, Exception innerException) {
        super(message, innerException);
    }
}
