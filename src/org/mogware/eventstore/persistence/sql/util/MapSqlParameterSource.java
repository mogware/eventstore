package org.mogware.eventstore.persistence.sql.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class MapSqlParameterSource extends AbstractSqlParameterSource {

    private final Map<String, Object> values = new HashMap<>();

    public MapSqlParameterSource() {
    }

    public MapSqlParameterSource(String paramName, Object value) {
        addValue(paramName, value);
    }

    public MapSqlParameterSource(Map<String, ?> values) {
        addValues(values);
    }

    public MapSqlParameterSource addValue(String paramName, Object value) {
        this.values.put(paramName, value);
        if (value instanceof SqlParameterValue)
            registerSqlType(paramName, ((SqlParameterValue)value).getSqlType());
        return this;
    }

    public MapSqlParameterSource addValue(String paramName, Object value, 
            int sqlType) {
        this.values.put(paramName, value);
        registerSqlType(paramName, sqlType);
        return this;
    }

    public MapSqlParameterSource addValue(String paramName, Object value, 
            int sqlType, String typeName) {
        this.values.put(paramName, value);
        registerSqlType(paramName, sqlType);
        registerTypeName(paramName, typeName);
        return this;
    }

    public MapSqlParameterSource addValues(Map<String, ?> values) {
        if (values != null) {
            values.entrySet().stream()
                .map((e) -> {
                    this.values.put(e.getKey(), e.getValue());
                    return e; })
                .filter((e) -> (e.getValue() instanceof SqlParameterValue))
                .forEach((e) -> {
                    SqlParameterValue value = (SqlParameterValue) e.getValue();
                    registerSqlType(e.getKey(), value.getSqlType()); });
        }
        return this;
    }

    public Map<String, Object> getValues() {
        return Collections.unmodifiableMap(this.values);
    }

    @Override
    public boolean hasValue(String paramName) {
        return this.values.containsKey(paramName);
    }

    @Override
    public Object getValue(String paramName) {
        if (!hasValue(paramName)) {
            throw new IllegalArgumentException(
                    "No value registered for key '" + paramName + "'"
            );
        }
        return this.values.get(paramName);
    }
}
