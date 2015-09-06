package org.mogware.eventstore.persistence.sql.sqldialects;

public class CommonSqlStatements {
    public static String getAppendSnapshotToCommit() {
        return
            "INSERT INTO Snapshots " +
                "( BucketId, StreamId, StreamRevision, Payload ) " +
            "SELECT @BucketId, @StreamId, @StreamRevision, @Payload " +
            "/*FROM DUAL*/ " +                
            "WHERE EXISTS " +
                "( SELECT * " +
                "    FROM Commits " +
                "   WHERE BucketId = @BucketId " +
                "     AND StreamId = @StreamId " +
                "     AND (StreamRevision - Items) <= @StreamRevision ) " +
            "AND NOT EXISTS " +
                "( SELECT * " +
                "    FROM Snapshots " +
                "   WHERE BucketId = @BucketId " +
                "     AND StreamId = @StreamId " +
                "     AND StreamRevision = @StreamRevision );";
    }
    
    public static String getDeleteStream() {
        return
            "DELETE FROM Snapshots " +
                "WHERE BucketId = @BucketId AND StreamId = @StreamId; " +
            "DELETE FROM Commits " +
                "WHERE BucketId = @BucketId AND StreamId = @StreamId;";
    }
    
    public static String getDropTables() {
        return
            "DROP TABLE Snapshots; " +
            "DROP TABLE Commits;";
    }
    
    public static String getDuplicateCommit() {
        return 
            "SELECT COUNT(*) FROM Commits " +
            "WHERE BucketId = @BucketId " +
            "  AND StreamId = @StreamId " +
            "  AND CommitSequence = @CommitSequence " +
            "  AND CommitId = @CommitId;";
    }
    
    public static String getGetCommitsFromCheckpoint() {
        return 
            "SELECT BucketId, StreamId, StreamIdOriginal, StreamRevision, " +
                "CommitId, CommitSequence, CommitStamp, CheckpointNumber, " +
                "Headers, Payload " +
            "FROM Commits " +
            "WHERE CheckpointNumber > @CheckpointNumber " +
            "ORDER BY CheckpointNumber " +
            "LIMIT @Limit OFFSET @Skip;";
    }
    
    public static String getGetCommitsFromInstant() {
        return
            "SELECT BucketId, StreamId, StreamIdOriginal, StreamRevision, " +
                "CommitId, CommitSequence, CommitStamp, CheckpointNumber, " +
                "Headers, Payload " +
            "FROM Commits " +
            "WHERE BucketId = @BucketId AND CommitStamp >= @CommitStamp " +
            "ORDER BY CommitStamp, StreamId, CommitSequence " +
            "LIMIT @Limit OFFSET @Skip;";
    }
    
    public static String getGetCommitsFromStartingRevision() {
        return
            "SELECT BucketId, StreamId, StreamIdOriginal, StreamRevision, " +
                "CommitId, CommitSequence, CommitStamp, CheckpointNumber, " +
                "Headers, Payload " +
            "FROM Commits " +
            "WHERE BucketId = @BucketId " +
            "  AND StreamId = @StreamId " +
            "  AND StreamRevision >= @StreamRevision " +
            "  AND (StreamRevision - Items) < @MaxStreamRevision " +
            "  AND CommitSequence > @CommitSequence " +
            "ORDER BY CommitSequence " +
            "LIMIT @Limit;";
    }
    
    public static String getGetCommitsFromToInstant() {
        return
            "SELECT BucketId, StreamId, StreamIdOriginal, StreamRevision, " +
                "CommitId, CommitSequence, CommitStamp, CheckpointNumber, " +
                "Headers, Payload " +
            "FROM Commits " +
            "WHERE BucketId = @BucketId " +
            "  AND CommitStamp >= @CommitStampStart " +
            "  AND CommitStamp < @CommitStampEnd " +
            "ORDER BY CommitStamp, StreamId, CommitSequence " +
            "LIMIT @Limit OFFSET @Skip;";
    }
    
    public static String getGetSnapshot() {
        return
            "SELECT * FROM Snapshots " +
            "WHERE BucketId = @BucketId " +
            "  AND StreamId = @StreamId " +
            "  AND StreamRevision <= @StreamRevision " +
            "ORDER BY StreamRevision DESC " +
            "LIMIT 1;";
    }
    
    public static String getGetStreamsRequiringSnapshots() {
        return
            "SELECT C.BucketId, C.StreamId, C.StreamIdOriginal, " +
                "MAX(C.StreamRevision) AS StreamRevision, " +
                "MAX(COALESCE(S.StreamRevision, 0)) AS SnapshotRevision " +
            "FROM Commits AS C " +
            "LEFT OUTER JOIN Snapshots AS S ON C.BucketId = @BucketId " +
                "AND C.StreamId = S.StreamId " +
                "AND C.StreamRevision >= S.StreamRevision " +
            "GROUP BY C.StreamId, C.BucketId, C.StreamIdOriginal " +
            "HAVING MAX(C.StreamRevision) >= " +
                "MAX(COALESCE(S.StreamRevision, 0)) + @Threshold " +
            "ORDER BY C.StreamId " +
            "LIMIT @Limit OFFSET @Skip;";
    }
    
    public static String getPurgeBucket() {
        return
            "DELETE FROM Snapshots WHERE BucketId = @BucketId; " +
            "DELETE FROM Commits WHERE BucketId = @BucketId;";
    }
    
    public static String getPurgeStorage() {
        return
            "DELETE FROM Snapshots; " +
            "DELETE FROM Commits;";
    }
}
