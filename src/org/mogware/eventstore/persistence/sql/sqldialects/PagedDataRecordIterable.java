package org.mogware.eventstore.persistence.sql.sqldialects;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.mogware.eventstore.persistence.StorageException;
import org.mogware.eventstore.persistence.sql.DataRecord;
import org.mogware.eventstore.persistence.sql.SqlDialect;
import org.mogware.eventstore.persistence.sql.util.ParsedSql;

public class PagedDataRecordIterable implements Iterable<DataRecord> {
    private final SqlDialect dialect;
    private final ParsedSql parsedSql;    
    private final PreparedStatement statement;
    private final NextPageDelegate nextpage;
    private final int pageSize; 

    public PagedDataRecordIterable(
            SqlDialect dialect,
            ParsedSql parsedSql,
            PreparedStatement statement,
            NextPageDelegate nextpage,
            int pageSize
    ) {
        this.dialect = dialect;
        this.parsedSql = parsedSql;
        this.statement = statement;
        this.nextpage = nextpage;
        this.pageSize = pageSize;
    }
    
    @Override
    public Iterator<DataRecord> iterator() {
        return new Itr();
    }
    
    private class Itr implements Iterator<DataRecord> {
        private List<DataRecord> cache;
        private int cursor;
        private int position;

        public Itr() {
            this.position = 0;
            this.cache = openNextPage();
        }

        @Override
        public boolean hasNext() {
            if (this.cursor < this.cache.size())
                return true;

            if (!this.pagingEnabled())
                return false;

            if (!pageCompletelyEnumerated())
                return false;

            DataRecord last = this.cache.get(this.cursor - 1);
            this.getNextPageDelegate().apply(
                    PagedDataRecordIterable.this.parsedSql, 
                    PagedDataRecordIterable.this.statement, last);
            PagedDataRecordIterable.this.nextpage.apply(
                    PagedDataRecordIterable.this.parsedSql,
                    PagedDataRecordIterable.this.statement, last);
            this.cache = openNextPage();

            return this.cursor < this.cache.size();
        }

        @Override
        public DataRecord next() {
            this.position++;
            return this.cache.get(this.cursor++);
        }

        private boolean pagingEnabled() {
            return PagedDataRecordIterable.this.pageSize > 0;
        }

        private boolean pageCompletelyEnumerated() {
            return this.position > 0 && 
                    0 == this.position % PagedDataRecordIterable.this.pageSize;
        }

        private List<DataRecord> openNextPage() {
            try {
                this.cursor = 0;
                return CommonSqlCommand.executeQuery(
                        PagedDataRecordIterable.this.statement
                );
            } catch (Exception ex) {
                throw new StorageException(ex.getMessage());
            }
        }

        private NextPageDelegate getNextPageDelegate() {
            return (p, s, r) -> {
                List<String> names =
                    PagedDataRecordIterable.this.parsedSql.getParameterNames();
                IntStream.range(0, names.size())
                        .filter((i) -> names.get(i).equals(
                                PagedDataRecordIterable.this.dialect.getSkip()))
                        .forEach((i) -> this.setSkip(i + 1, this.position));
            };
        }

        private void setSkip(int i, int s) {
            try {
                PagedDataRecordIterable.this.statement.setInt(i, s);
            } catch (SQLException ex) {
                throw new StorageException(ex.getMessage());
            }
        }
    }
}
