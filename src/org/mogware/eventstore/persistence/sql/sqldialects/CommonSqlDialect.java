package org.mogware.eventstore.persistence.sql.sqldialects;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.IntStream;
import org.mogware.eventstore.persistence.StorageException;
import org.mogware.eventstore.persistence.sql.ConnectionFactory;
import org.mogware.eventstore.persistence.sql.DataRecord;
import org.mogware.eventstore.persistence.sql.SqlCommand;
import org.mogware.eventstore.persistence.sql.SqlDialect;

public abstract class CommonSqlDialect implements SqlDialect {
    @Override
    public abstract String getInitializeStorage();

    @Override
    public String getAppendSnapshotToCommit() {
        return CommonSqlStatements.getAppendSnapshotToCommit();
    }

    @Override
    public String getDeleteStream() {
        return CommonSqlStatements.getDeleteStream();
    }

    @Override
    public String getDrop() {
        return CommonSqlStatements.getDropTables();
    }

    @Override
    public String getDuplicateCommit() {
        return CommonSqlStatements.getDuplicateCommit();
    }

    @Override
    public String getGetCommitsFromInstant() {
        return CommonSqlStatements.getGetCommitsFromInstant();
    }

    @Override
    public String getGetCommitsFromStartingRevision() {
        return CommonSqlStatements.getGetCommitsFromStartingRevision();
    }

    @Override
    public String getGetCommitsFromCheckpoint() {
        return CommonSqlStatements.getGetCommitsFromCheckpoint();
    }

    @Override
    public String getGetCommitsFromToInstant() {
        return CommonSqlStatements.getGetCommitsFromToInstant();
    }

    @Override
    public String getGetSnapshot() {
        return CommonSqlStatements.getGetSnapshot();
    }

    @Override
    public String getGetStreamsRequiringSnapshots() {
        return CommonSqlStatements.getGetStreamsRequiringSnapshots();
    }

    @Override
    public String getPurgeBucket() {
        return CommonSqlStatements.getPurgeBucket();
    }

    @Override
    public String getPurgeStorage() {
        return CommonSqlStatements.getPurgeStorage();
    }

    @Override
    public String getBucketId() {
        return "@BucketId";
    }

    @Override
    public String getStreamId() {
        return "@StreamId";
    }

    @Override
    public String getStreamIdOriginal() {
        return "@StreamIdOriginal";
    }

    @Override
    public String getStreamRevision() {
        return "@StreamRevision";
    }

    @Override
    public String getMaxStreamRevision() {
        return "@MaxStreamRevision";
    }

    @Override
    public String getItems() {
        return "@Items";
    }

    @Override
    public String getCommitId() {
        return "@CommitId";
    }

    @Override
    public String getCommitSequence() {
        return "@CommitSequence";
    }

    @Override
    public String getCommitStamp() {
        return "@CommitStamp";
    }

    @Override
    public String getCommitStampStart() {
        return "@CommitStampStart";
    }

    @Override
    public String getCommitStampEnd() {
        return "@CommitStampEnd";
    }

    @Override
    public String getHeaders() {
        return "@Headers";
    }

    @Override
    public String getPayload() {
        return "@Payload";
    }

    @Override
    public String getThreshold() {
        return "@Threshold";
    }

    @Override
    public String getCheckpointNumber() {
        return "@CheckpointNumber";
    }

    @Override
    public String getLimit() {
        return "@Limit";
    }

    @Override
    public String getSkip() {
        return "@Skip";
    }

    @Override
    public boolean getCanPage() {
        return true;
    }

    @Override
    public boolean isDuplicate(Exception exception) {
        String message = exception.getMessage().toUpperCase();
        return message.contains("DUPLICATE") ||
                message.contains("UNIQUE") ||
                message.contains("CONSTRAINT");
    }

    @Override
    public void addPayloadParamater(ConnectionFactory connectionFactory,
            Connection connection, SqlCommand cmd, byte[] payload) {
        cmd.addParameter(getPayload(), payload);
    }

    @Override
    public NextPageDelegate getNextPageDelegate() {
        return (p, s, r) -> {
            List<String> names = p.getParameterNames();
            IntStream.range(0, names.size())
                .filter((i) -> names.get(i).equals(this.getCommitSequence()))
                .forEach((i) -> this.setCommitSequence(s, i + 1, r));
        };
    }

    private void setCommitSequence(PreparedStatement s, int i, DataRecord r) {
        try {
            s.setInt(i, (int) r.get("commitsequence"));
        } catch (SQLException ex) {
            throw new StorageException(ex.getMessage());
        }
    }

    @Override
    public SqlCommand buildCommand(Connection connection) {
        return new CommonSqlCommand(this, connection);
    }
}
