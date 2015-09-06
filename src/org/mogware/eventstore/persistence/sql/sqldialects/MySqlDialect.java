package org.mogware.eventstore.persistence.sql.sqldialects;

import java.util.Properties;

public class MySqlDialect extends CommonSqlDialect {
    @Override
    public String getInitializeStorage() {
        return MySqlStatements.getInitializeStorage();
    }
    
    @Override    
    public String getAppendSnapshotToCommit() {
        return super.getAppendSnapshotToCommit()
                .replace("/*FROM DUAL*/", "FROM DUAL");
    }

    @Override
    public String getPersistCommit() {
        return MySqlStatements.getPersistCommit();
    }
    
    @Override
    public String getConnectionURL(Properties prop) {
        String host = prop.getProperty("host", "127.0.0.1");
        String port = prop.getProperty("port", "3306");
        String database = prop.getProperty("database", "eventstore");
        return "jdbc:mysql://" + host + ":" + port + "/" +
                database + "?allowMultiQueries=true";
    }    
}
