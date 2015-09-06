package org.mogware.eventstore.persistence.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.mogware.eventstore.persistence.StorageUnavailableException;

public class JdbcConnectionFactory implements ConnectionFactory {
    private static final Set<String> loadedDrivers = new HashSet<>();
    
    private final String driverClassName;
    
    public JdbcConnectionFactory(String driverClassName) {
        this.driverClassName = driverClassName;
    }
    
    @Override
    public Connection open(String url, String user, String password) {
        try {
            if (this.driverClassName != null)
                loadDriverClass(this.driverClassName);
            return java.sql.DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException ex) {
            throw new StorageUnavailableException(
                    "could not load driver for class name " +
                    this.driverClassName
            );            
        } catch (SQLException ex) {
            throw new StorageUnavailableException(ex.getMessage());
        }        
    }
    
    public static void loadDriverClass(String driverClassName)
            throws ClassNotFoundException {
        if (loadedDrivers.contains(driverClassName))
            return;
        Class.forName(driverClassName);
        loadedDrivers.add(driverClassName);
    }
}
