package org.mogware.eventstore.persistence.sql.util;

import java.util.ArrayList;
import java.util.List;

public class ParsedSql {
    private final String originalSql;
    private final List<String> parameterNames = new ArrayList<>();
    private final List<int[]> parameterIndexes = new ArrayList<>();
    private int namedParameterCount;
    private int unnamedParameterCount;
    private int totalParameterCount;
    
    public ParsedSql(String originalSql) {
        this.originalSql = originalSql;
    }
    
    public String getOriginalSql() {
        return this.originalSql;
    }
    
    public void addNamedParameter(String parameterName, int startIndex, int endIndex) {
        this.parameterNames.add(parameterName);
        this.parameterIndexes.add(new int[] {startIndex, endIndex});
    }
    
    public List<String> getParameterNames() {
        return this.parameterNames;
    }
        
    int[] getParameterIndexes(int parameterPosition) {
        return this.parameterIndexes.get(parameterPosition);
    }
        
    void setNamedParameterCount(int namedParameterCount) {
        this.namedParameterCount = namedParameterCount;
    }

    int getNamedParameterCount() {
        return this.namedParameterCount;
    }
    
    void setUnnamedParameterCount(int unnamedParameterCount) {
        this.unnamedParameterCount = unnamedParameterCount;
    }

    int getUnnamedParameterCount() {
        return this.unnamedParameterCount;
    }

    void setTotalParameterCount(int totalParameterCount) {
        this.totalParameterCount = totalParameterCount;
    }

    int getTotalParameterCount() {
        return this.totalParameterCount;
    }
        
    @Override
    public String toString() {
        return this.originalSql;
    }
}
