package org.mogware.eventstore.persistence.sql;

import org.mogware.eventstore.persistence.sql.sqldialects.NextPageDelegate;

public interface SqlCommand {
        int getPageSize();
        void setPageSize(int size);
        
        void addParameter(String name, Object value);
        
        int executeNonQuery(String commandText);
        
        int executeWithoutExceptions(String commandText);
        
        Object executeScalar(String queryText);

        Iterable<DataRecord> executeWithQuery(String queryText);

        Iterable<DataRecord> executePagedQuery(String queryText,
                NextPageDelegate nextpage);
}
