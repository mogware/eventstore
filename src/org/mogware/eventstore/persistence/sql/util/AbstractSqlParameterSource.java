package org.mogware.eventstore.persistence.sql.util;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractSqlParameterSource 
        implements SqlParameterSource {
    private final Map<String, Integer> sqlTypes = new HashMap<>();
    private final Map<String, String> typeNames = new HashMap<>();

    public void registerSqlType(String paramName, int sqlType) {
        this.sqlTypes.put(paramName, sqlType);
    }

    public void registerTypeName(String paramName, String typeName) {
        this.typeNames.put(paramName, typeName);
    }

    public int getSqlType(String paramName) {
        Integer sqlType = this.sqlTypes.get(paramName);
        if (sqlType != null)
            return sqlType;
        return TYPE_UNKNOWN;
    }

    public String getTypeName(String paramName) {
        return this.typeNames.get(paramName);
    }
}
