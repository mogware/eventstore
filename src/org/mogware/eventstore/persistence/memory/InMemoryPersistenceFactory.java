package org.mogware.eventstore.persistence.memory;

import org.mogware.eventstore.persistence.PersistStreams;
import org.mogware.eventstore.persistence.PersistenceFactory;

public class InMemoryPersistenceFactory implements PersistenceFactory {
    @Override
    public PersistStreams build() {
        return new InMemoryPersistenceEngine();
    }
}
