package org.mogware.eventstore.persistence.sql;

public class UniqueKeyViolationException extends RuntimeException {
    public UniqueKeyViolationException() {
    }
    public UniqueKeyViolationException(String message) {
        super(message);
    }
    public UniqueKeyViolationException(String message, Exception innerException) {
        super(message, innerException);
    }
}
