package org.mogware.eventstore.persistence;

import java.util.Properties;
import org.junit.Test;
import org.mogware.eventstore.persistence.sql.JdbcConnectionFactory;
import org.mogware.eventstore.persistence.sql.Sha1StreamIdHasher;
import org.mogware.eventstore.persistence.sql.SqlPersistenceFactory;
import org.mogware.eventstore.persistence.sql.sqldialects.MySqlDialect;
import org.mogware.eventstore.serialization.JsonSerializer;

public class MySqlPersistenceTest extends TestCasePersistence {
    private static final PersistStreams engine = new SqlPersistenceFactory(
            new JdbcConnectionFactory("com.mysql.jdbc.Driver"),
            new MySqlDialect(),
            new JsonSerializer(),
            new Sha1StreamIdHasher(),
            new Properties() {{
                setProperty("host", "127.0.0.1");
                setProperty("port", "3306");
                setProperty("database", "estest");
                setProperty("user", "root");
                setProperty("password", "Mogware1");
            }},
            2).build();
    static {
        engine.initialize();
    }

    @Override
    protected PersistStreams persistence() {
        return engine;
    }

    @Test
    public void persistenceCommitHeader() {
        System.out.println("MySqlPersistenceTest: persistenceCommitHeader");
        assertPersistenceCommitHeader();
    }

    @Test
    public void persistenceCommitEvents() {
        System.out.println("MySqlPersistenceTest: persistenceCommitEvents");
        assertPersistenceCommitEvents();
    }

    @Test
    public void purgingCommitsInMultipleBuckets() {
        System.out.println("MySqlPersistenceTest: purgingCommitsInMultipleBuckets");
        assertPurgingCommitsInMultipleBuckets();
    }

    @Test
    public void persistenceLargePayload() {
        System.out.println("MySqlPersistenceTest: persistenceLargePayload");
        assertPersistenceLargePayload();
    }
}
