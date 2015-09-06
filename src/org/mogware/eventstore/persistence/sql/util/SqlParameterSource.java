package org.mogware.eventstore.persistence.sql.util;

public interface SqlParameterSource {
    /**
     * Constant that indicates an unknown (or unspecified) SQL type.
     */
    public static final int TYPE_UNKNOWN = Integer.MIN_VALUE;    

    /**
     * Determine whether there is a value for the specified named parameter.
     */
    public boolean hasValue(String paramName);

    /**
     * Return the parameter value for the requested named parameter.
     */
    public Object getValue(String paramName) throws IllegalArgumentException;

    /**
     * Determine the SQL type for the specified named parameter.
     */
    public int getSqlType(String paramName);

    /**
     * Determine the type name for the specified named parameter.
     */
    public String getTypeName(String paramName);
}
