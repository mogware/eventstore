package org.mogware.eventstore;

public class DuplicateCommitException extends RuntimeException {
    public DuplicateCommitException() {
    }
    public DuplicateCommitException(String message) {
        super(message);
    }
    public DuplicateCommitException(String message, Exception innerException) {
        super(message, innerException);
    }
}
