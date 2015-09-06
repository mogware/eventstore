package org.mogware.eventstore.persistence;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import org.mogware.eventstore.EventMessage;
import org.mogware.eventstore.Commit;
import org.mogware.system.Guid;

public class DefaultCommit implements Commit {
    private final String bucketId;
    private final String streamId;
    private final int streamRevision;
    private final Guid commitId;
    private final int commitSequence;
    private final LocalDateTime commitStamp;
    private final Map<String, Object> headers;
    private final List<EventMessage> events;
    private final String checkpointToken;

    public DefaultCommit(String bucketId, String streamId, int streamRevision,
            Guid commitId, int commitSequence, LocalDateTime commitStamp,
            String checkpointToken, Map<String,Object> headers,
            List<EventMessage> events) {
        this.bucketId = bucketId;
        this.streamId = streamId;
        this.streamRevision = streamRevision;
        this.commitId = commitId;
        this.commitSequence = commitSequence;
        this.commitStamp = commitStamp;
        this.checkpointToken = checkpointToken;
        this.headers = headers != null ? headers : new HashMap<>();
        this.events = events != null ?  events : new ArrayList<>();
    }
    
    @Override
    public String getBucketId() {
        return this.bucketId;
    }

    @Override
    public String getStreamId() {
        return this.streamId;
    }

    @Override
    public int getStreamRevision() {
        return this.streamRevision;
    }

    @Override
    public Guid getCommitId() {
        return this.commitId;
    }

    @Override
    public int getCommitSequence() {
        return this.commitSequence;
    }

    @Override
    public LocalDateTime getCommitStamp() {
        return this.commitStamp;
    }

    @Override
    public Map<String, Object> getHeaders() {
        return this.headers;
    }

    @Override
    public List<EventMessage> getEvents() {
        return this.events;
    }

    @Override
    public String getCheckpointToken() {
        return this.checkpointToken;
    }
    
}
