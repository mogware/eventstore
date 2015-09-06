package org.mogware.eventstore.persistence;

public class StorageUnavailableException extends StorageException {
    public StorageUnavailableException() {
    }
    public StorageUnavailableException(String message) {
        super(message);
    }
    public StorageUnavailableException(String message, Exception innerException) {
        super(message, innerException);
    }
}
