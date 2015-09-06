package org.mogware.eventstore.persistence.sql;

import java.util.Properties;
import org.mogware.eventstore.persistence.PersistStreams;
import org.mogware.eventstore.persistence.PersistenceFactory;
import org.mogware.eventstore.serialization.Serialize;

public class SqlPersistenceFactory implements PersistenceFactory {
    private final ConnectionFactory connectionFactory;
    private final SqlDialect dialect;
    private final Serialize serializer;    
    private final Properties authority;
    private final int pageSize;
    private final StreamIdHasher streamIdHasher;
    
    public SqlPersistenceFactory(
            ConnectionFactory factory,
            SqlDialect dialect,
            Serialize serializer,
            StreamIdHasher streamIdHasher,
            Properties authority,
            int pageSize
    ) {
        this.connectionFactory = factory;  
        this.dialect = dialect;
        this.serializer = serializer;
        this.streamIdHasher = streamIdHasher;
        this.authority = authority;
        this.pageSize = pageSize;
    }
    
    @Override
    public PersistStreams build() {
        return new SqlPersistenceEngine(
            this.connectionFactory,
            this.dialect,
            this.serializer,
            this.authority,
            this.pageSize,
            this.streamIdHasher
        );
    }
}
