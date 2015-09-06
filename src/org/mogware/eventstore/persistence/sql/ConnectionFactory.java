package org.mogware.eventstore.persistence.sql;

import java.sql.Connection;

public interface ConnectionFactory {
    Connection open(String url, String user, String password);
}
