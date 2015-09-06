package org.mogware.eventstore.persistence.sql.sqldialects;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mogware.eventstore.persistence.StorageException;
import org.mogware.eventstore.persistence.sql.DataRecord;
import org.mogware.eventstore.persistence.sql.SqlCommand;
import org.mogware.eventstore.persistence.sql.SqlDialect;
import org.mogware.eventstore.persistence.sql.UniqueKeyViolationException;
import org.mogware.eventstore.persistence.sql.util.SqlParameterSource;
import org.mogware.eventstore.persistence.sql.util.MapSqlParameterSource;
import org.mogware.eventstore.persistence.sql.util.NamedParameter;
import org.mogware.eventstore.persistence.sql.util.ParsedSql;

public class CommonSqlCommand implements SqlCommand {
    private final Connection connection;
    private final SqlDialect dialect;
    private int pageSize = 0;
    
    protected final Map<String, Object> parameters = new HashMap<>();

    public CommonSqlCommand(SqlDialect dialect, Connection connection) {
        this.connection = connection;
        this.dialect = dialect;
    }
    
    @Override
    public int getPageSize() {
        return this.pageSize;
    }

    @Override
    public void setPageSize(int size) {
        this.pageSize = size;
    }
    
    @Override    
    public void addParameter(String name, Object value) {
        this.parameters.put(name, value);
    }    
    
    @Override
    public int executeNonQuery(String commandText) {
        try {
            ParsedSql parsedSql = NamedParameter.parseSqlStatement(commandText);                        
            PreparedStatement statement = this.buildStatement(parsedSql);
            return statement.executeUpdate();
        } catch (Exception ex) {
            if (this.dialect.isDuplicate(ex))
                throw new UniqueKeyViolationException(ex.getMessage());
            throw new StorageException(ex.getMessage());
        }
    }

    @Override
    public int executeWithoutExceptions(String commandText) {
        try {
           this.executeNonQuery(commandText);
        } catch (Exception ex) {}
        return 0;
    }

    @Override
    public Object executeScalar(String queryText) {
        try {
            ParsedSql parsedSql = NamedParameter.parseSqlStatement(queryText);                                    
            PreparedStatement statement = this.buildStatement(parsedSql);
            return valueResultFromStatement(statement.executeQuery());            
        } catch (Exception ex) {
            if (this.dialect.isDuplicate(ex))
                throw new UniqueKeyViolationException(ex.getMessage());
            throw new StorageException(ex.getMessage());
        }
    }

    @Override
    public Iterable<DataRecord> executeWithQuery(String queryText) {
        try {
            ParsedSql parsedSql = NamedParameter.parseSqlStatement(queryText);            
            return executeQuery(this.buildStatement(parsedSql));            
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }
    }
    
    public Iterable<DataRecord> executePagedQuery(String queryText,
                NextPageDelegate nextpage) {
        int pageSize = this.dialect.getCanPage() ? this.getPageSize() : 0;
        if (pageSize > 0)
            this.parameters.put(this.dialect.getLimit(), pageSize);
        try {
            this.parameters.put(this.dialect.getSkip(), 0);
            ParsedSql parsedSql = NamedParameter.parseSqlStatement(queryText);                        
            return new PagedDataRecordIterable(
                this.dialect,
                parsedSql,
                this.buildStatement(parsedSql),
                nextpage,
                pageSize
            );
        } catch (Exception ex) {
            throw new StorageException(ex.getMessage());
        }        
    }
    
    public static List<DataRecord> executeQuery(
            PreparedStatement statement) throws SQLException {
        return resultsFromStatement(statement.executeQuery());                    
    }
            
    protected PreparedStatement buildStatement(ParsedSql parsedSql)
            throws SQLException {
        PreparedStatement statement = this.connection.prepareStatement(
                NamedParameter.substituteNamedParameters(parsedSql, null)
        );
        SqlParameterSource source = new MapSqlParameterSource(this.parameters);
        prepareStatement(statement,
                NamedParameter.buildValueArray(parsedSql, source, null),
                NamedParameter.buildSqlTypeArray(parsedSql, source)
        );
        return statement;
    }
    
    protected static void prepareStatement(PreparedStatement statement,
            Object[] parameters, int[] types) throws SQLException {
        if (parameters == null || parameters.length == 0)
            return;
        ParameterMetaData metadata = null;
        for (int i = 1; i <= parameters.length; i++) {
            final Object parameter = parameters[i - 1];
            int sqltype = types[i - 1];
            if (sqltype != SqlParameterSource.TYPE_UNKNOWN)
                statement.setObject(i, parameter, sqltype);
            else if (parameter == null) {
                if (metadata == null)
                    metadata = statement.getParameterMetaData();
                try {
                    sqltype = metadata.getParameterType(i);
                } catch (SQLException ex) {
                    sqltype = java.sql.Types.NULL;
                }
                statement.setNull(i, sqltype);
            } else {
                final Class type = parameter.getClass();
                if (type == Boolean.class || type == boolean.class) {
                    statement.setBoolean(i, (Boolean) parameter);
                } else if (type == Byte.class || type == byte.class) {
                    statement.setByte(i, (Byte) parameter);
                } else if (type == Short.class || type == short.class) {
                    statement.setShort(i, (Short) parameter);
                } else if (type == Integer.class || type == int.class) {
                    statement.setInt(i, (Integer) parameter);
                } else if (type == Long.class || type == long.class) {
                    statement.setLong(i, (Long) parameter);
                } else if (type == Float.class || type == float.class) {
                    statement.setFloat(i, (Float) parameter);
                } else if (type == Double.class || type == double.class) {
                    statement.setDouble(i, (Double) parameter);
                } else if (type == Character.class || type == char.class) {
                    statement.setString(i, parameter == null ? null : "" + (Character) parameter);
                } else if (type == char[].class) {
                    statement.setString(i, parameter == null ? null : new String((char[]) parameter));
                } else if (type == Character[].class) {
                    final Character[] src = (Character[]) parameter;
                    final char[] dst = new char[src.length];
                    for (int j = 0; j < src.length; j++) {
                        dst[j] = src[j];
                    }
                    statement.setString(i, new String(dst));
                } else if (type == String.class) {
                    statement.setString(i, (String) parameter);
                } else if (type == BigDecimal.class) {
                    statement.setBigDecimal(i, (BigDecimal) parameter);
                } else if (type == byte[].class) {
                    statement.setBytes(i, (byte[]) parameter);
                } else if (type == Byte[].class) {
                    final Byte[] src = (Byte[]) parameter;
                    final byte[] dst = new byte[src.length];
                    for (int j = 0; j < src.length; j++) {
                        dst[j] = src[j];
                    }
                    statement.setBytes(i, dst);
                } else if (type == org.mogware.system.Guid.class) {
                    final org.mogware.system.Guid guid =
                            (org.mogware.system.Guid) parameter;
                    statement.setBytes(i, guid.toByteArray());
                } else if (type == java.time.LocalDate.class) {
                    final java.time.LocalDate date =
                            (java.time.LocalDate) parameter;
                    statement.setDate(i, java.sql.Date.valueOf(date));
                } else if (type == java.time.LocalTime.class) {
                    final java.time.LocalTime time =
                            (java.time.LocalTime) parameter;
                    statement.setTime(i, java.sql.Time.valueOf(time));
                } else if (type == java.time.LocalDateTime.class) {
                    final java.time.LocalDateTime datetime =
                            (java.time.LocalDateTime) parameter;
                    statement.setTimestamp(i, Timestamp.from(
                            datetime.toInstant(ZoneOffset.UTC)
                    ));
                } else if (type == java.time.ZonedDateTime.class) {
                    final java.time.ZonedDateTime datetime =
                            (java.time.ZonedDateTime) parameter;                    
                    statement.setTimestamp(i, Timestamp.from(
                            datetime.toInstant()
                    ));
                } else if (type == java.util.Date.class) {
                    final java.util.Date date = (java.util.Date) parameter;
                    statement.setTimestamp(i, 
                            new java.sql.Timestamp(date.getTime())
                    );
                } else if (type == java.sql.Date.class) {
                    statement.setDate(i, (java.sql.Date) parameter);
                } else if (type == java.sql.Time.class) {
                    statement.setTime(i, (java.sql.Time) parameter);
                } else if (type == java.sql.Timestamp.class) {
                    statement.setTimestamp(i, (java.sql.Timestamp) parameter);
                } else {
                    statement.setObject(i, parameter);
                }
            }
        }
    }

    protected static List<DataRecord> resultsFromStatement(
            final ResultSet resultSet) throws SQLException {
        final List<DataRecord> ret = new ArrayList<>();
        while (resultSet.next())
            ret.add(resultsFromReslutSet(resultSet));
        return ret;
    }
    
    protected static Object valueResultFromStatement(final ResultSet resultSet)
            throws SQLException {
        final int type = resultSet.getMetaData().getColumnType(1);
        if (! resultSet.next())
            return null;
        final Object ret = getValueFromResultSet(resultSet, 1, type);
        if (resultSet.next())
            throw new StorageException("Non-unique result returned");
        return ret;
    }

    private static DataRecord resultsFromReslutSet(
            final ResultSet resultSet) throws SQLException {
        final DataRecord ret = new DataRecord();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            final String columnName = metaData.getColumnName(i).toLowerCase();
            final int type = metaData.getColumnType(i);
            final Object value = getValueFromResultSet(resultSet, i, type);
            ret.put(columnName, value);
        }
	return ret;
    }
    
    protected static Object getValueFromResultSet(final ResultSet resultSet,
            final int column, final int type) throws SQLException {
        Object value = null;
        if (type == java.sql.Types.ARRAY)
            value = resultSet.getArray(column);
        else if (type == java.sql.Types.BIGINT)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getLong(column);
        else if (type == java.sql.Types.BINARY)
            value = resultSet.getBytes(column);
        else if (type == java.sql.Types.BIT)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getBoolean(column);
        else if (type == java.sql.Types.BLOB)
            value = resultSet.getBytes(column);
        else if (type == java.sql.Types.BOOLEAN)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getBoolean(column);
        else if (type == java.sql.Types.CHAR)
            value = resultSet.getString(column);
        else if (type == java.sql.Types.CLOB)
            value = resultSet.getString(column);
        else if (type == java.sql.Types.DATALINK)
            value = resultSet.getBinaryStream(column);
        else if (type == java.sql.Types.DATE)
            value = resultSet.getDate(column).toLocalDate();
        else if (type == java.sql.Types.DECIMAL)
            value = resultSet.getBigDecimal(column);
        else if (type == java.sql.Types.DOUBLE)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getDouble(column);
        else if (type == java.sql.Types.FLOAT)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getFloat(column);
        else if (type == java.sql.Types.INTEGER)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getInt(column);
        else if (type == java.sql.Types.JAVA_OBJECT)
            value = resultSet.getObject(column);
        else if (type == java.sql.Types.LONGVARBINARY)
            value = resultSet.getBytes(column);
        else if (type == java.sql.Types.LONGVARCHAR)
            value = resultSet.getString(column);
        else if (type == java.sql.Types.NULL)
            value = null;
        else if (type == java.sql.Types.NUMERIC)
            value = resultSet.getBigDecimal(column);
        else if (type == java.sql.Types.OTHER)
            value = resultSet.getObject(column);
        else if (type == java.sql.Types.REAL)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getDouble(column);
        else if (type == java.sql.Types.REF)
            value = resultSet.getRef(column);
        else if (type == java.sql.Types.SMALLINT)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getInt(column);
        else if (type == java.sql.Types.TIME)
            value = resultSet.getTime(column).toLocalTime();
        else if (type == java.sql.Types.TIMESTAMP)
            value = resultSet.getTimestamp(column).toLocalDateTime();
        else if (type == java.sql.Types.TINYINT)
            value = resultSet.getObject(column) == null ? 
                    null : resultSet.getInt(column);
        else if (type == java.sql.Types.VARBINARY)
            value = resultSet.getBytes(column);
        else if (type == java.sql.Types.VARCHAR)
            value = resultSet.getString(column);
        return value;
    }    
}
