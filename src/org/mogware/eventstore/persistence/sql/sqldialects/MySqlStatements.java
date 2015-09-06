package org.mogware.eventstore.persistence.sql.sqldialects;

public class MySqlStatements {
    public static String getInitializeStorage() {
        return
            "CREATE TABLE IF NOT EXISTS Commits " +
            "( " +
            "    BucketId varchar(40) charset utf8 NOT NULL, " +
            "    StreamId varchar(40) charset utf8 NOT NULL, " +
            "    StreamIdOriginal varchar(1000) charset utf8 NOT NULL, " +
            "    StreamRevision int NOT NULL CHECK (StreamRevision > 0), " +
            "    Items tinyint NOT NULL CHECK (Items > 0), " +
            "    CommitId binary(16) NOT NULL CHECK (CommitId != 0), " +
            "    CommitSequence int NOT NULL CHECK (CommitSequence > 0), " +
            "    CommitStamp timestamp NOT NULL, " +
            "    CheckpointNumber bigint AUTO_INCREMENT, " +
            "    Dispatched bit NOT NULL DEFAULT 0, " +
            "    Headers blob NULL, " +
            "    Payload mediumblob NOT NULL, " +
            "    PRIMARY KEY (CheckpointNumber) " +
            "); " +
            "CREATE UNIQUE INDEX IX_Commits " +
            "    ON Commits (BucketId, StreamId, CommitSequence); " +
            "CREATE UNIQUE INDEX IX_Commits_CommitId " +
            "    ON Commits (BucketId, StreamId, CommitId); " +
            "CREATE UNIQUE INDEX IX_Commits_Revisions " +
            "    ON Commits (BucketId, StreamId, StreamRevision, Items); " +
            "CREATE INDEX IX_Commits_Dispatched ON Commits (Dispatched); " +
            "CREATE INDEX IX_Commits_Stamp ON Commits (CommitStamp); " +
            "CREATE TABLE IF NOT EXISTS Snapshots " +
            "( " +
            "    BucketId varchar(40) charset utf8 NOT NULL, " +
            "    StreamId varchar(40) charset utf8 NOT NULL, " +
            "    StreamRevision int NOT NULL CHECK (StreamRevision > 0), " +
            "    Payload blob NOT NULL, " +
            "    CONSTRAINT PK_Snapshots " +
            "        PRIMARY KEY (BucketId, StreamId, StreamRevision) " + 
            ");";
    }

    public static String getPersistCommit() {
        return
            "INSERT INTO Commits " +
            "    ( BucketId, StreamId, StreamIdOriginal, CommitId, " +
            "      CommitSequence, StreamRevision, Items, CommitStamp, " +
            "      Headers, Payload ) " +
            "VALUES " +
            "    ( @BucketId, @StreamId, @StreamIdOriginal, @CommitId, " +
            "      @CommitSequence, @StreamRevision, @Items, @CommitStamp, " +
            "      @Headers, @Payload ); " +
            "SELECT LAST_INSERT_ID();";
    }
}
