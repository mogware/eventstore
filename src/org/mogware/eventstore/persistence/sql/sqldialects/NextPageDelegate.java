package org.mogware.eventstore.persistence.sql.sqldialects;

import java.sql.PreparedStatement;
import org.mogware.eventstore.persistence.sql.DataRecord;
import org.mogware.eventstore.persistence.sql.util.ParsedSql;

@FunctionalInterface
public interface NextPageDelegate {
    void apply(
        ParsedSql parsedSql, PreparedStatement statement, DataRecord current
    );
}
