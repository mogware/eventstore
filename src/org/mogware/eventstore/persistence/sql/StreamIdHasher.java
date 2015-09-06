package org.mogware.eventstore.persistence.sql;

public interface StreamIdHasher {
    public String GetHash(String streamId);
}
