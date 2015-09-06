package org.mogware.eventstore.persistence.sql.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class NamedParameter {
    private static final char[] PARAMETER_SEPARATORS = new char[] {
        '"', '\'', ':', '&', ',', ';', '(', ')', '|', '=', '+', '-', '*', '%',
        '/', '\\', '<', '>', '^'
    };
    private static final String[] START_SKIP = new String[] {
        "'", "\"", "--", "/*"
    };
    private static final String[] STOP_SKIP = new String[] {
        "'", "\"", "\n", "*/"
    };
    
    public static ParsedSql parseSqlStatement(String sql) {
        Set<String> namedParameters = new HashSet<>();
        ParsedSql parsedSql = new ParsedSql(sql);

        char[] statement = sql.toCharArray();
        int namedParameterCount = 0;
        int unnamedParameterCount = 0;
        int totalParameterCount = 0;
        
        int i = 0;
        while (i < statement.length) {
            int skipToPosition = skipCommentsAndQuotes(statement, i);
            if (i != skipToPosition) {
                if (skipToPosition >= statement.length)
                    break;
                i = skipToPosition;
            }
            char c = statement[i];
            if (c == '@') {
                int j = i + 1;
                while (j < statement.length && 
                        !isParameterSeparator(statement[j])) {
                    j++;
                }
                if (j - i > 1) {
                    String parameter = sql.substring(i, j);
                    if (!namedParameters.contains(parameter)) {
                        namedParameters.add(parameter);
                        namedParameterCount++;
                    }
                    parsedSql.addNamedParameter(parameter, i, j);
                    totalParameterCount++;
                }
                i = j - 1;
            } else if (c == '?') {
                unnamedParameterCount++;
                totalParameterCount++;
            }
            i++;
        }
        parsedSql.setNamedParameterCount(namedParameterCount);
        parsedSql.setUnnamedParameterCount(unnamedParameterCount);
        parsedSql.setTotalParameterCount(totalParameterCount);
        return parsedSql;        
    }
    
    public static String substituteNamedParameters(ParsedSql parsedSql, 
            SqlParameterSource paramSource) {
        String originalSql = parsedSql.getOriginalSql();
        StringBuilder actualSql = new StringBuilder();
        List paramNames = parsedSql.getParameterNames();
        int lastIndex = 0;
        for (int i = 0; i < paramNames.size(); i++) {
            String paramName = (String) paramNames.get(i);
            int[] indexes = parsedSql.getParameterIndexes(i);
            int startIndex = indexes[0];
            int endIndex = indexes[1];
            actualSql.append(originalSql.substring(lastIndex, startIndex));
            if (paramSource != null && paramSource.hasValue(paramName)) {
                Object value = paramSource.getValue(paramName);
                if (value instanceof Collection) {
                    Iterator entryIter = ((Collection) value).iterator();
                    int k = 0;
                    while (entryIter.hasNext()) {
                        if (k > 0)
                            actualSql.append(", ");
                        k++;
                        Object entryItem = entryIter.next();
                        if (entryItem instanceof Object[]) {
                            Object[] expressionList = (Object[]) entryItem;
                            actualSql.append("(");
                            for (int m = 0; m < expressionList.length; m++) {
                                if (m > 0)
                                    actualSql.append(", ");
                                actualSql.append("?");
                            }
                            actualSql.append(")");
                        } else
                            actualSql.append("?");
                    }
                } else
                    actualSql.append("?");
            } else
                actualSql.append("?");
            lastIndex = endIndex;
        }
        actualSql.append(originalSql.substring(
                lastIndex, originalSql.length())
        );
        return actualSql.toString();
    }

    public static Object[] buildValueArray(ParsedSql parsedSql, 
        SqlParameterSource paramSource, List<SqlParameter> declaredParams) {

        Object[] paramArray = new Object[parsedSql.getTotalParameterCount()];
        if (parsedSql.getNamedParameterCount() > 0 && 
                parsedSql.getUnnamedParameterCount() > 0) {
            throw new IllegalStateException(
                "You can't mix named and traditional placeholders. You have " +
                parsedSql.getNamedParameterCount() +
                    " named parameter(s) and " +
                parsedSql.getUnnamedParameterCount() + 
                    " traditonal placeholder(s) in [" +
                parsedSql.getOriginalSql() + "]");
        }
        List<String> paramNames = parsedSql.getParameterNames();
        for (int i = 0; i < paramNames.size(); i++) {
            String paramName = paramNames.get(i);
            try {
                Object value = paramSource.getValue(paramName);
                SqlParameter param = findParameter(declaredParams,paramName,i);
                paramArray[i] = param != null ? 
                        new SqlParameterValue(param, value) : value;
            } catch (IllegalArgumentException ex) {
                throw new IllegalStateException(
                    "No value supplied for the SQL parameter '" + paramName +
                            "': " + ex.getMessage());
            }
        }
        return paramArray;
    }

    public static int[] buildSqlTypeArray(ParsedSql parsedSql,
            SqlParameterSource paramSource) {
        int[] sqlTypes = new int[parsedSql.getTotalParameterCount()];
        List paramNames = parsedSql.getParameterNames();
        for (int i = 0; i < paramNames.size(); i++) {
            String paramName = (String) paramNames.get(i);
            sqlTypes[i] = paramSource.getSqlType(paramName);
        }
        return sqlTypes;
    }
    
    public static String parseSqlStatementIntoString(String sql) {
        ParsedSql parsedSql = parseSqlStatement(sql);
        return substituteNamedParameters(parsedSql, null);
    }

    public static String substituteNamedParameters(String sql, 
            SqlParameterSource paramSource) {
        ParsedSql parsedSql = parseSqlStatement(sql);
        return substituteNamedParameters(parsedSql, paramSource);
    }

    public static Object[] buildValueArray(String sql, Map<String, ?> paramMap) {
        ParsedSql parsedSql = parseSqlStatement(sql);
        return buildValueArray(
                parsedSql, new MapSqlParameterSource(paramMap), null
        );
    }
    
    private static int skipCommentsAndQuotes(char[] statement, int position) {
        for (int i = 0; i < START_SKIP.length; i++) {
            if (statement[position] == START_SKIP[i].charAt(0)) {
                boolean match = true;
                for (int j = 1; j < START_SKIP[i].length(); j++) {
                    if (!(statement[position + j] == START_SKIP[i].charAt(j))) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    int offset = START_SKIP[i].length();
                    for (int m = position + offset; m < statement.length; m++) {
                        if (statement[m] == STOP_SKIP[i].charAt(0)) {
                            boolean endMatch = true;
                            int endPos = m;
                            for (int n = 1; n < STOP_SKIP[i].length(); n++) {
                                if (m + n >= statement.length)
                                    return statement.length;
                                if (!(statement[m + n] == 
                                        STOP_SKIP[i].charAt(n))) {
                                    endMatch = false;
                                    break;
                                }
                                endPos = m + n;
                            }
                            if (endMatch)
                                return endPos + 1;
                        }
                    }
                    return statement.length;
                }
            }
        }
        return position;
    }
    
    private static boolean isParameterSeparator(char c) {
        if (Character.isWhitespace(c))
            return true;
        for (char separator : PARAMETER_SEPARATORS) {
            if (c == separator)
                return true;
        }
        return false;
    }

    private static SqlParameter findParameter(
        List<SqlParameter> declaredParams, String paramName, int paramIndex) {
        if (declaredParams != null) {
            for (SqlParameter declaredParam : declaredParams) {
                if (paramName.equals(declaredParam.getName())) {
                    return declaredParam;
                }
            }
            if (paramIndex < declaredParams.size()) {
                SqlParameter declaredParam = declaredParams.get(paramIndex);
                if (declaredParam.getName() == null) {
                    return declaredParam;
                }
            }
        }
        return null;
    }
}
