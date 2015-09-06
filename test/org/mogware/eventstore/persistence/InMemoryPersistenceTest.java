package org.mogware.eventstore.persistence;

import org.junit.Test;
import org.mogware.eventstore.persistence.memory.InMemoryPersistenceFactory;

public class InMemoryPersistenceTest extends TestCasePersistence {
    private static final PersistStreams engine =
            new InMemoryPersistenceFactory().build();

    @Override
    protected PersistStreams persistence() {
        return engine;
    }

    @Test
    public void persistenceCommitHeader() {
        System.out.println("InMemoryPersistenceTest: persistenceCommitHeader");
        assertPersistenceCommitHeader();
    }

    @Test
    public void persistenceCommitEvents() {
        System.out.println("InMemoryPersistenceTest: persistenceCommitEvents");
        assertPersistenceCommitEvents();
    }

    @Test
    public void purgingCommitsInMultipleBuckets() {
        System.out.println("InMemoryPersistenceTest: purgingCommitsInMultipleBuckets");
        assertPurgingCommitsInMultipleBuckets();
    }

    @Test
    public void persistenceLargePayload() {
        System.out.println("InMemoryPersistenceTest: persistenceLargePayload");
        assertPersistenceLargePayload();
    }
}
