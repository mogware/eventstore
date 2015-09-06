package org.mogware.eventstore;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.mogware.eventstore.persistence.StorageException;

public class OptimisticPipelineHook extends PipelineHookBase {
    private static final int MaxStreamsToTrack = 100;
    private final HashMap<HeadKey, Commit> heads = new HashMap<>();
    private final LinkedList<HeadKey> maxItemsToTrack = new LinkedList<>();
    private final int maxStreamsToTrack;

    public OptimisticPipelineHook() {
        this(MaxStreamsToTrack);
    }
    
    public OptimisticPipelineHook(int maxStreamsToTrack) {
        this.maxStreamsToTrack = maxStreamsToTrack;
    }
    
    @Override
    public Commit select(Commit committed) {
        track(committed);
        return committed;
    }
    
    @Override
    public boolean preCommit(CommitAttempt attempt) {
        Commit head = this.getStreamHead(getHeadKey(attempt));
        if (head == null)
            return true;
         
        if (head.getCommitSequence() >= attempt.getCommitSequence())
            throw new ConcurrencyException();
         
        if (head.getStreamRevision() >= attempt.getStreamRevision())
            throw new ConcurrencyException();
         
        if (head.getCommitSequence() < attempt.getCommitSequence() - 1)
            throw new StorageException();
         
        if (head.getStreamRevision() < attempt.getStreamRevision() - 
                attempt.getEvents().size())
            throw new StorageException();
         
        return true;
    }
    
    @Override
    public void postCommit(Commit committed) {
        track(committed);
    }
    
    @Override
    public void onPurge(String bucketId) {
        synchronized (this.maxItemsToTrack)
        {
            if (bucketId == null) {
                this.heads.clear();
                this.maxItemsToTrack.clear();
            } else {
                List<HeadKey> headsInBucket = this.heads.keySet().stream()
                        .filter((k) -> k.getBucketId().equals(bucketId))
                        .collect(Collectors.toList());
                headsInBucket.stream().forEach((head) -> {
                    removeHead(head);
                });
            }
        }
    }
    
    @Override
    public void onDeleteStream(String bucketId, String streamId) {
        synchronized (this.maxItemsToTrack) {
            removeHead(new HeadKey(bucketId, streamId));
        }
    }
    
    public void track(Commit committed) {
        if (committed == null)
            return;
        synchronized (this.maxItemsToTrack) {
            this.updateStreamHead(committed);
            this.trackUpToCapacity(committed);
        }
    }
    
    private void updateStreamHead(Commit committed) {
        HeadKey headKey = getHeadKey(committed);
        Commit head = this.getStreamHead(headKey);
        if (alreadyTracked(head))
            this.maxItemsToTrack.remove(headKey);
        head = head != null ? head : committed;
        head = head.getStreamRevision() > committed.getStreamRevision() ?
                head : committed;
        this.heads.put(headKey, head);
        
    }
    
    private void removeHead(HeadKey headKey) {
        this.heads.remove(headKey);
        this.maxItemsToTrack.remove(headKey);
    }

    private static boolean alreadyTracked(Commit head) {
        return head != null;
    }

    private void trackUpToCapacity(Commit committed) {
        this.maxItemsToTrack.addFirst(getHeadKey(committed));
        if (this.maxItemsToTrack.size() <= this.maxStreamsToTrack)
            return;
        HeadKey expired = this.maxItemsToTrack.getLast();
        this.heads.remove(expired);
        this.maxItemsToTrack.removeLast();
    }
    
    public boolean contains(Commit attempt) {
        return this.getStreamHead(getHeadKey(attempt)) != null;
    }

    private Commit getStreamHead(HeadKey headKey) {
        synchronized (this.maxItemsToTrack) {
            return this.heads.get(headKey);
        }
    }
    
    private static HeadKey getHeadKey(Commit commit) {
        return new HeadKey(commit.getBucketId(), commit.getStreamId());
    }
    
    private static HeadKey getHeadKey(CommitAttempt commitAttempt) {
        return new HeadKey(
            commitAttempt.getBucketId(), commitAttempt.getStreamId()
        );        
    }

    private final static class HeadKey {
        private final String bucketId;
        private final String streamId;
        
        public HeadKey(String bucketId, String streamId) {
            this.bucketId = bucketId;
            this.streamId = streamId;
        }
        
        public String getBucketId() {
            return this.bucketId;
        }

        public String getStreamId() {
            return this.streamId;
        }

        protected boolean equals(HeadKey other) {
            return this.bucketId.equals(other.bucketId) &&
                    this.streamId.equals(other.streamId);
        }
        
        @Override
        public boolean equals(Object other) {            
            if (this == other)
                return true;
            if (other == null || (this.getClass() != other.getClass()))
                return false;
            return this.equals((HeadKey) other);
        }
        
        @Override
        public int hashCode() {
            return (this.bucketId.hashCode() * 397) ^ this.streamId.hashCode();            
        }        
    }
}
