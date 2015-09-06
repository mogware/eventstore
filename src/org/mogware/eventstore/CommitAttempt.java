package org.mogware.eventstore;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mogware.system.Guid;

public class CommitAttempt {
    private final String bucketId;
    private final String streamId;
    private final int streamRevision;
    private final Guid commitId;
    private final int commitSequence;
    private final LocalDateTime  commitStamp;
    private final Map<String, Object> headers;
    private final List<EventMessage> events;
    
    public CommitAttempt(Guid streamId, int streamRevision, Guid commitId,
            int commitSequence, LocalDateTime  commitStamp,
            Map<String, Object> headers, List<EventMessage> events) {
        this(Bucket.Default, streamId.toString(), streamRevision, commitId,
                commitSequence, commitStamp, headers, events);
    }
    
    public CommitAttempt(String streamId, int streamRevision, Guid commitId,
            int commitSequence, LocalDateTime commitStamp,
            Map<String, Object> headers, List<EventMessage> events) {
        this(Bucket.Default, streamId, streamRevision, commitId,
                commitSequence, commitStamp, headers, events);
    }
    
    public CommitAttempt(String bucketId, String streamId, int streamRevision,
            Guid commitId, int commitSequence, LocalDateTime commitStamp,
            Map<String, Object> headers, List<EventMessage> events) {
        this.bucketId = bucketId;
        this.streamId = streamId;
        this.streamRevision = streamRevision;
        this.commitId = commitId;
        this.commitSequence = commitSequence;
        this.commitStamp = commitStamp;
        this.headers = headers != null ? headers : new HashMap<>();
        this.events = events != null ? events : new ArrayList<>();
    }
    
    public String getBucketId() {
        return this.bucketId;
    }

    public String getStreamId() {
        return this.streamId;
    }

    public int getStreamRevision() {
        return this.streamRevision;
    }

    public Guid getCommitId() {
        return this.commitId;
    }

    public int getCommitSequence() {
        return this.commitSequence;
    }

    public LocalDateTime getCommitStamp() {
        return this.commitStamp;
    }

    public Map<String,Object> getHeaders() {
        return this.headers;
    }

    public List<EventMessage> getEvents() {
        return this.events;
    }
}
