package org.mogware.eventstore.persistence.sql;

import java.sql.Connection;
import java.util.Properties;
import org.mogware.eventstore.persistence.sql.sqldialects.NextPageDelegate;

public interface SqlDialect {
    String getInitializeStorage();
    String getPurgeStorage();
    String getPurgeBucket();
    String getDrop();
    String getDeleteStream();
    
    String getGetCommitsFromStartingRevision();
    String getGetCommitsFromInstant();
    String getGetCommitsFromToInstant();
    String getGetCommitsFromCheckpoint();
    
    String getPersistCommit();
    String getDuplicateCommit();
    
    String getGetStreamsRequiringSnapshots();
    String getGetSnapshot();
    String getAppendSnapshotToCommit();

    String getBucketId();
    String getStreamId();
    String getStreamIdOriginal();
    String getStreamRevision();
    String getMaxStreamRevision();
    String getItems();
    String getCommitId();
    String getCommitSequence();
    String getCommitStamp();
    String getCommitStampStart();
    String getCommitStampEnd();
    String getHeaders();
    String getPayload();
    String getThreshold();
    String getCheckpointNumber();
    
    String getLimit();
    String getSkip();
    boolean getCanPage();  
    
    String getConnectionURL(Properties prop);
    
    boolean isDuplicate(Exception exception);
    
    void addPayloadParamater(ConnectionFactory connectionFactory,
            Connection connection, SqlCommand cmd, byte[] payload);
    
    NextPageDelegate getNextPageDelegate();
    
    SqlCommand buildCommand(Connection connection);
}
