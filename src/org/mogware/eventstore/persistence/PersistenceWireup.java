package org.mogware.eventstore.persistence;

import org.mogware.eventstore.EventStore;
import org.mogware.eventstore.serialization.SerializationWireup;
import org.mogware.eventstore.TransactionScopeOption;
import org.mogware.eventstore.Wireup;
import org.mogware.eventstore.persistence.sql.ConnectionFactory;
import org.mogware.eventstore.persistence.sql.JdbcConnectionFactory;
import org.mogware.eventstore.persistence.sql.SqlPersistenceWireup;
import org.mogware.eventstore.serialization.BsonSerializer;
import org.mogware.eventstore.serialization.CborSerializer;
import org.mogware.eventstore.serialization.Serialize;
import org.mogware.eventstore.serialization.JsonSerializer;

public class PersistenceWireup extends Wireup {
    private boolean initialize = false;

    public PersistenceWireup(Wireup inner) {
        super(inner);
        this.getContainer().register(Serialize.class, new JsonSerializer());        
    }

    public PersistenceWireup withPersistence(PersistStreams instance)
    {
        this.with(PersistStreams.class, instance);
        return this;
    }

    public SqlPersistenceWireup usingSqlPersistence(String driverClass) {
        ConnectionFactory factory = new JdbcConnectionFactory(driverClass);
        return new SqlPersistenceWireup(this, factory);
    }

    public SqlPersistenceWireup usingSqlPersistence(
            ConnectionFactory factory) {
        return new SqlPersistenceWireup(this, factory);
    }

    protected SerializationWireup withSerializer(Serialize instance) {
        return new SerializationWireup(this, instance);
    }

    public SerializationWireup usingJsonSerialization() {
        return new SerializationWireup(this, new JsonSerializer());
    }

    public SerializationWireup usingBsonSerialization() {
        return new SerializationWireup(this, new BsonSerializer());
    }

    public SerializationWireup usingCborSerialization() {
        return new SerializationWireup(this, new CborSerializer());
    }

    public PersistenceWireup initializeStorageEngine() {
        this.initialize = true;
        return this;
    }

    @Override
    public EventStore build() {
        System.out.println("build from PersistenceWireup");
        PersistStreams engine = (PersistStreams)
                this.getContainer().resolve(PersistStreams.class);
        if (this.initialize)
            engine.initialize();
        return super.build();
    }
}
