package org.mogware.eventstore;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mogware.system.Guid;

public class OptimisticEventStream implements EventStream {
    private final List<EventMessage> committed = new LinkedList<>();
    private final CommitEvents persistence;
    private final List<EventMessage> events = new LinkedList<>();
    private final Set<Guid> identifiers = new HashSet<>();
    private final Map<String, Object> committedHeaders = new HashMap<>();
    private final Map<String, Object> uncommittedHeaders = new HashMap<>();    
    
    public OptimisticEventStream(String bucketId, String streamId,
            CommitEvents persistence) {
        this.bucketId = bucketId;
        this.streamId = streamId;
        this.persistence = persistence;
    }

    public OptimisticEventStream(String bucketId, String streamId,
            CommitEvents persistence, int minRevision, int maxRevision) {
        this(bucketId, streamId, persistence);
        Iterable<Commit> commits = persistence.getFrom(
                bucketId, streamId, minRevision, maxRevision
        );
        populateStream(minRevision, maxRevision, commits);
        if (minRevision > 0 && this.committed.isEmpty())
            throw new StreamNotFoundException();
    }
    
    public OptimisticEventStream(Snapshot snapshot, CommitEvents persistence,
            int maxRevision) {
        this(snapshot.getBucketId(), snapshot.getStreamId(), persistence);
        Iterable<Commit> commits = persistence.getFrom(
                snapshot.getBucketId(), snapshot.getStreamId(),
                snapshot.getStreamRevision(), maxRevision
        );
        populateStream(snapshot.getStreamRevision() + 1, maxRevision, commits);
        this.streamRevision = snapshot.getStreamRevision() + 
                this.committed.size();
    }
    
    private String bucketId;
    public String getBucketId() {
        return this.bucketId;
    }

    public void setBucketId(String value) {
        this.bucketId = value;
    }

    private String streamId;
    @Override
    public String getStreamId() {
        return this.streamId;
    }

    public void setStreamId(String value) {
        this.streamId = value;
    }
    
    private int streamRevision;
    @Override
    public int getStreamRevision() {
        return this.streamRevision;
    }

    public void setStreamRevision(int value) {
        this.streamRevision = value;
    }

    private int commitSequence;
    @Override
    public int getCommitSequence() {
        return this.commitSequence;
    }

    public void setCommitSequence(int value) {
        this.commitSequence = value;
    }
    
    @Override
    public List<EventMessage> getCommittedEvents() {
        return Collections.unmodifiableList(this.committed);
    }

    @Override
    public Map<String, Object> getCommittedHeaders() {
        return Collections.unmodifiableMap(this.committedHeaders);
    }

    @Override
    public List<EventMessage> getUncommittedEvents() {
        return Collections.unmodifiableList(this.events);
    }

    @Override
    public Map<String, Object> getUncommittedHeaders() {
        return Collections.unmodifiableMap(this.uncommittedHeaders);        
    }

    @Override
    public void add(EventMessage uncommittedEvent) {
        if (uncommittedEvent == null || uncommittedEvent.getBody() == null)
            return;
        this.events.add(uncommittedEvent);
    }

    @Override
    public void commitChanges(Guid commitId) {
        if (this.identifiers.contains(commitId))
            throw new DuplicateCommitException();
        if (!hasChanges())
            return;
        try {
            persistChanges(commitId);
        } catch (ConcurrencyException ex) {
            Iterable<Commit> commits = this.persistence.getFrom(this.bucketId,
                    this.streamId, this.streamRevision + 1, Integer.MAX_VALUE);
            this.populateStream(this.streamRevision + 1, 
                    Integer.MAX_VALUE, commits);
            throw ex;
        }
    }

    @Override
    public void clearChanges() {
        this.events.clear();
        this.uncommittedHeaders.clear();
    }
    
    private boolean hasChanges() {
        return !this.events.isEmpty();
    }
    
    private void populateStream(int minRevision, int maxRevision,
            Iterable<Commit> commits) {
        for (Commit commit : commits) {
            this.identifiers.add(commit.getCommitId());
            setCommitSequence(commit.getCommitSequence());
            int currentRevision = commit.getStreamRevision() -
                    commit.getEvents().size() + 1;
            if (currentRevision > maxRevision)
                return ;
            copyToCommittedHeaders(commit);
            copyToEvents(minRevision, maxRevision, currentRevision, commit);
        }
    }
    
    private void copyToCommittedHeaders(Commit commit) {
        commit.getHeaders().keySet().stream().forEach((key) -> {
            this.committedHeaders.put(key, commit.getHeaders().get(key));
        });
    }
    
    private void copyToEvents(int minRevision, int maxRevision,
            int currentRevision, Commit commit) {
        for (EventMessage event : commit.getEvents()) {
            if (currentRevision > maxRevision)
                break;
            if (currentRevision++ < minRevision) {
                continue;
            }
            this.committed.add(event);
            setStreamRevision(currentRevision - 1);
        }
    }
    
    private void persistChanges(Guid commitId) {
        CommitAttempt attempt = buildCommitAttempt(commitId);
        Commit commit = this.persistence.commit(attempt);
        if (commit != null)
            populateStream(getStreamRevision() + 1, attempt.getStreamRevision(),
                    new ArrayList<Commit>() {{ add(commit); }});
        clearChanges();
    }
    
    private CommitAttempt buildCommitAttempt(Guid commitId) {
        return new CommitAttempt(getBucketId(), getStreamId(),
                getStreamRevision() + this.events.size(), commitId,
                getCommitSequence() + 1, LocalDateTime.now(),
                new HashMap<>(this.uncommittedHeaders),
                new LinkedList<>(this.events)
        );
    }    
}
