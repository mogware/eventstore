package org.mogware.eventstore.persistence.sql;

import java.util.Properties;
import org.mogware.eventstore.persistence.PersistenceWireup;
import org.mogware.eventstore.Wireup;
import org.mogware.eventstore.persistence.PersistStreams;
import org.mogware.eventstore.serialization.Serialize;

public class SqlPersistenceWireup extends PersistenceWireup {
    private static final int DefaultPageSize = 512;
    private int pageSize = DefaultPageSize;
    private Properties authority = new Properties();
    
    public SqlPersistenceWireup(Wireup inner, ConnectionFactory factory) {
        super(inner);
        this.getContainer().register(SqlDialect.class, (c) -> null);
        this.getContainer().register(
                StreamIdHasher.class, (c) -> new Sha1StreamIdHasher()
        );                
        this.getContainer().register(PersistStreams.class,
            (c) -> new SqlPersistenceFactory(factory,
                (SqlDialect) c.resolve(SqlDialect.class),
                (Serialize) c.resolve(Serialize.class),                    
                (StreamIdHasher) c.resolve(StreamIdHasher.class),                    
                this.authority,
                this.pageSize
            ).build());
    }
    
    public SqlPersistenceWireup withDialect(SqlDialect instance) {
        this.getContainer().register(SqlDialect.class, instance);
        return this;
    }
    
    public SqlPersistenceWireup withStreamIdHasher(StreamIdHasher instance) {
        this.getContainer().register(StreamIdHasher.class, instance);
        return this;
    }
    
    public SqlPersistenceWireup withConnection(String host, int port,
            String database) {
        this.authority.setProperty("host", host);
        this.authority.setProperty("port", Integer.toString(port));
        this.authority.setProperty("database", database);        
        return this;
        
    }
    public SqlPersistenceWireup withCredentials(String user, String password) {
        this.authority.setProperty("user", user);
        this.authority.setProperty("password", password);        
        return this;
    }    
    
    public SqlPersistenceWireup pageEvery(int records) {
        this.pageSize = records;
        return this;
    }    
}
