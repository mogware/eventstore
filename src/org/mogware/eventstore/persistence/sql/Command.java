package org.mogware.eventstore.persistence.sql;

import java.sql.Connection;

@FunctionalInterface
public interface Command<R> {
    R apply(Connection connection, SqlCommand command);
}
