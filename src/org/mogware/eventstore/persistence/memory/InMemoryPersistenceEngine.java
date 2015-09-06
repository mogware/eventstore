package org.mogware.eventstore.persistence.memory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mogware.system.Guid;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap; 
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.mogware.eventstore.CommitAttempt;
import org.mogware.eventstore.ConcurrencyException;
import org.mogware.eventstore.DuplicateCommitException;
import org.mogware.eventstore.EventMessage;
import org.mogware.eventstore.Checkpoint;
import org.mogware.eventstore.Commit;
import org.mogware.eventstore.Snapshot;
import org.mogware.eventstore.LongCheckpoint;
import org.mogware.eventstore.persistence.DefaultCommit;
import org.mogware.eventstore.persistence.PersistStreams;
import org.mogware.eventstore.persistence.StreamHead;
import org.mogware.eventstore.persistence.DefaultStreamHead;

public class InMemoryPersistenceEngine implements PersistStreams {
    private final ConcurrentMap<String, Bucket> buckets  = 
            new ConcurrentHashMap<>();
    private final AtomicLong checkpoint = new AtomicLong();
    
    private Bucket get(String bucketId) {
        if (! this.buckets.containsKey(bucketId))
            this.buckets.put(bucketId, new Bucket());
        return this.buckets.get(bucketId);
    }
    
    @Override
    public void initialize() {
        System.out.println("Init InMemoryPersistenceEngine");
    }

    @Override
    public Iterable<Commit> getFrom(String bucketId, String streamId,
            int minRevision, int maxRevision) {
        return (Iterable<Commit>) this.get(bucketId)
                .getFrom(streamId, minRevision, maxRevision);
    }

    @Override
    public Commit commit(CommitAttempt attempt) {
        return this.get(attempt.getBucketId()).commit(
                attempt, new LongCheckpoint(this.checkpoint.incrementAndGet())
        );
    }

    @Override
    public Snapshot getSnapshot(String bucketId, String streamId,
            int maxRevision) {
        return this.get(bucketId).getSnapshot(streamId, maxRevision);

    }

    @Override
    public boolean addSnapshot(Snapshot snapshot) {
        return this.get(snapshot.getBucketId()).addSnapshot(snapshot);
    }

    @Override
    public Iterable<StreamHead> getStreamsToSnapshot(String bucketId,
            int maxThreshold) {
        return (Iterable<StreamHead>) this.get(bucketId)
                .getStreamsToSnapshot(maxThreshold);
    }

    @Override
    public Iterable<Commit> getFrom(String bucketId, LocalDateTime start) {
        return (Iterable<Commit>) this.get(bucketId).getFrom(start);
    }

    @Override
    public Iterable<Commit> getFrom(String checkpointToken) {
        Checkpoint checkpoint = LongCheckpoint.parse(checkpointToken);
        List<? extends Commit> commits = this.buckets.values().stream()
                .flatMap((b) -> b.getCommits().stream())
                .filter((c) -> c.getCheckpoint().compareTo(checkpoint) > 0)
                .sorted((c1, c2) -> c1.getCheckpoint()
                        .compareTo(c2.getCheckpoint()))                
                .collect(Collectors.toList());
        return (Iterable<Commit>) commits;
    }

    @Override
    public Checkpoint getCheckpoint(String checkpointToken) {
        return LongCheckpoint.parse(checkpointToken);
    }

    @Override
    public List<Commit> getFromTo(String bucketId, LocalDateTime start,
            LocalDateTime end) {
        return (List<Commit>) this.get(bucketId).getFromTo(start, end);
    }

    @Override
    public void purge() {
        this.buckets.values().stream().forEach((bucket) -> {
            bucket.purge();
        });        
    }

    @Override
    public void purge(String bucketId) {
        this.buckets.remove(bucketId);
    }

    @Override
    public void drop() {
        this.buckets.clear();
    }

    @Override
    public void deleteStream(String bucketId, String streamId) {
        Bucket bucket = this.buckets.get(bucketId);
        if (bucket == null)
            return;
        bucket.deleteStream(streamId);
    }
    
    private static class IdentityForDuplicationDetection   
    {
        private final int commitSequence;
        private final Guid commitId;
        private final String bucketId;
        private final String streamId;
        
        public IdentityForDuplicationDetection(CommitAttempt commitAttempt) {
            this.bucketId = commitAttempt.getBucketId();
            this.streamId = commitAttempt.getStreamId();
            this.commitId = commitAttempt.getCommitId();
            this.commitSequence = commitAttempt.getCommitSequence();
        }

        public IdentityForDuplicationDetection(Commit commit) {
            this.bucketId = commit.getBucketId();
            this.streamId = commit.getStreamId();
            this.commitId = commit.getCommitId();
            this.commitSequence = commit.getCommitSequence();
        }
        
        protected boolean equals(IdentityForDuplicationDetection other) {
                return this.streamId.equals(other.streamId) && 
                        this.bucketId.equals(other.bucketId) &&
                        this.commitSequence == other.commitSequence &&
                        this.commitId.equals(other.commitId);
        }
        
        @Override
        public boolean equals(Object other) {        
            if (this == other)
                return true;
            if (other == null || (this.getClass() != other.getClass()))
                return false;
            return this.equals((IdentityForDuplicationDetection) other);
        }
        
        @Override
        public int hashCode() {
            int hashCode = this.streamId.hashCode();
            hashCode = (hashCode * 397) ^ this.bucketId.hashCode();
            hashCode = (hashCode * 397) ^ this.commitSequence;
            hashCode = (hashCode * 397) ^ this.commitId.hashCode();
            return hashCode;
        }        
    }

    private static class IdentityForConcurrencyConflictDetection {
        private final int commitSequence;
        private final String streamId;

        public IdentityForConcurrencyConflictDetection(Commit commit) {
            this.streamId = commit.getStreamId();
            this.commitSequence = commit.getCommitSequence();
        }
        
        protected boolean equals(IdentityForConcurrencyConflictDetection other) {
            return this.commitSequence == other.commitSequence &&
                    this.streamId.equals(other.streamId);
        }
        
        @Override
        public boolean equals(Object other) {        
            if (this == other)
                return true;
            if (other == null || (this.getClass() != other.getClass()))
                return false;
            return this.equals((IdentityForConcurrencyConflictDetection) other);
        }
        
        @Override
        public int hashCode() {
            return (this.commitSequence * 397) ^ this.streamId.hashCode();            
        }
    }
    
    private static class InMemoryCommit extends DefaultCommit {
        private final Checkpoint checkpoint;
        public InMemoryCommit(String bucketId, String streamId,
                int streamRevision, Guid commitId, int commitSequence,
                LocalDateTime commitStamp, String checkpointToken,
                Map<String, Object> headers, List<EventMessage> events,
                Checkpoint checkpoint) {
            super(bucketId, streamId, streamRevision, commitId, commitSequence,
                    commitStamp, checkpointToken, headers, events);
            this.checkpoint = checkpoint;
        }
        
        public Checkpoint getCheckpoint() {
            return this.checkpoint;
        }
    }
    
    private static class Bucket {
        private final List<InMemoryCommit> commits = new ArrayList<>();
        private final Set<IdentityForDuplicationDetection>
                potentialDuplicates = new HashSet<>();
        private final Set<IdentityForConcurrencyConflictDetection>
                potentialConflicts = new HashSet<>();
        
        public List<InMemoryCommit> getCommits() {
            synchronized (this.commits) {
                return new ArrayList<>(this.commits);
            }            
        }
        
        private final List<StreamHead> heads = new LinkedList<>();
        private final List<Snapshot> snapshots = new LinkedList<>();
        private final Map<Guid, LocalDateTime> stamps = new HashMap<>();
        private final List<Commit> undispatched = new LinkedList<>();
                
        public List<? extends Commit> getFrom(String streamId, int minRevision,
                int maxRevision) {
            synchronized (this.commits) {
                return this.commits.stream()
                    .filter((x) -> x.getStreamId().equals(streamId) && 
                            x.getStreamRevision() >= minRevision &&
                            (x.getStreamRevision() - x.getEvents().size() + 1)
                                    <= maxRevision)
                    .sorted((e1, e2) -> Integer.compare(e1.getCommitSequence(),
                            e2.getCommitSequence()))
                    .collect(Collectors.toList());
            }
        }
        
        public List<? extends Commit> getFrom(LocalDateTime start) {
            Guid commitId = this.stamps.entrySet().stream()
                    .filter((x) -> x.getValue().equals(start) || 
                            x.getValue().isAfter(start))
                    .map((x) -> x.getKey())
                    .findFirst().orElse(null);
            if (commitId == null)
                return new ArrayList<>();
            InMemoryCommit startingCommit = this.commits.stream()
                    .filter((x) -> x.getCommitId().equals(commitId))
                    .findFirst().orElse(null);
            if (startingCommit == null)
                return new ArrayList<>();            
            return this.commits.subList(
                    this.commits.indexOf(startingCommit),
                    this.commits.size());
        }
        
        public List<? extends Commit> getFromTo(LocalDateTime start,
                LocalDateTime end) {
            List<Guid> selectedCommitIds = this.stamps.entrySet().stream()
                    .filter((x) -> x.getValue().isBefore(end) &&
                            (x.getValue().equals(start) ||
                                x.getValue().isAfter(start)))
                    .map((x) -> x.getKey())
                    .collect(Collectors.toList());
            Guid firstCommitId = selectedCommitIds.isEmpty() ?
                    null: selectedCommitIds.get(0);
            Guid lastCommitId = selectedCommitIds.isEmpty() ?
                    null: selectedCommitIds.get(selectedCommitIds.size() - 1);
            if (firstCommitId == null && lastCommitId == null)
                return new ArrayList<>();
            InMemoryCommit startingCommit = this.commits.stream()
                    .filter((x) -> x.getCommitId().equals(firstCommitId))
                    .findFirst().orElse(null);
            InMemoryCommit endingCommit = this.commits.stream()
                    .filter((x) -> x.getCommitId().equals(lastCommitId))
                    .findFirst().orElse(null);
            int startingCommitIndex = startingCommit == null ? 
                    0 : this.commits.indexOf(startingCommit);
            int endingCommitIndex = endingCommit == null ?
                    this.commits.size() : this.commits.indexOf(endingCommit)+1;            
            return this.commits.subList(startingCommitIndex, endingCommitIndex);            
        }
        
        public Commit commit(CommitAttempt attempt, Checkpoint checkpoint) {
            synchronized (this.commits) {
                detectDuplicate(attempt);
                InMemoryCommit commit = new InMemoryCommit(
                    attempt.getBucketId(),
                    attempt.getStreamId(),
                    attempt.getStreamRevision(),
                    attempt.getCommitId(),
                    attempt.getCommitSequence(),
                    attempt.getCommitStamp(),
                    checkpoint.getValue(),
                    attempt.getHeaders(),
                    attempt.getEvents(),
                    checkpoint
                );
                detectConflict(commit);
                this.stamps.put(commit.getCommitId(), commit.getCommitStamp());
                this.commits.add(commit);
                this.potentialDuplicates.add(
                        new IdentityForDuplicationDetection(commit)
                );
                this.potentialConflicts.add(
                        new IdentityForConcurrencyConflictDetection(commit)
                );
                this.undispatched.add(commit);
                StreamHead head = this.heads.stream()
                    .filter((x) -> x.getStreamId().equals(commit.getStreamId()))
                    .findFirst().orElse(null);
                this.heads.remove(head);
                int snapshotRevision = head == null ?
                        0 : head.getSnapshotRevision();
                this.heads.add(new DefaultStreamHead(
                        commit.getBucketId(),
                        commit.getStreamId(),
                        commit.getStreamRevision(),
                        snapshotRevision
                ));
                return commit;
            }
        }
        
        private void detectDuplicate(CommitAttempt attempt) {
            if (this.potentialDuplicates.contains(
                    new IdentityForDuplicationDetection(attempt)))
                throw new DuplicateCommitException();
        }
        
        private void detectConflict(Commit commit) {
            if (this.potentialConflicts.contains(
                    new IdentityForConcurrencyConflictDetection(commit)))
                throw new ConcurrencyException();
        }

        public List<Commit> getUndispatchedCommits() {
            synchronized (this.commits) {
                return this.commits.stream()
                    .filter((x) -> this.undispatched.contains(x))
                    .sorted((e1, e2) -> Integer.compare(e1.getCommitSequence(),
                            e2.getCommitSequence()))
                    .collect(Collectors.toList());                        
            }            
        }

        public void markCommitAsDispatched(Commit commit) {
            synchronized (this.commits) {            
                this.undispatched.remove(commit);
            }
        }
        
        public List<? extends StreamHead> getStreamsToSnapshot(int maxThreshold) {
            synchronized (this.commits) {
                return this.heads.stream()
                        .filter((x) -> x.getHeadRevision() >=
                              x.getSnapshotRevision() + maxThreshold)
                        .map((x) -> new DefaultStreamHead(
                            x.getBucketId(),
                            x.getStreamId(),
                            x.getHeadRevision(),
                            x.getSnapshotRevision()))
                        .collect(Collectors.toList());
            }
        }
        
        public Snapshot getSnapshot(String streamId,
                int maxRevision) {
            synchronized (this.commits) {
                return this.snapshots.stream()
                        .filter((x) -> x.getStreamId().equals(streamId) &&
                                x.getStreamRevision() <= maxRevision)
                        .findFirst().orElse(null);                        
            }
        }
         
        public boolean addSnapshot(Snapshot snapshot) {
            synchronized (this.commits) {
                StreamHead currentHead = this.heads.stream()
                        .filter((h) -> h.getStreamId().equals(
                                snapshot.getStreamId()))
                        .findFirst().orElse(null);
                if (currentHead == null)
                    return false;
                this.snapshots.add(snapshot);
                this.heads.remove(currentHead);
                this.heads.add(new DefaultStreamHead(
                        currentHead.getBucketId(),
                        currentHead.getStreamId(),
                        currentHead.getHeadRevision(),
                        snapshot.getStreamRevision()
                        
                ));
            }
            return true;
        }
        
        public void purge() {
            synchronized (this.commits) {
                this.commits.clear();
                this.snapshots.clear();
                this.heads.clear();
                this.potentialConflicts.clear();
                this.potentialDuplicates.clear();
            }
        }
        
        public void deleteStream(String streamId) {
            synchronized (this.commits) {
                List<InMemoryCommit> commits = this.commits.stream()
                        .filter((c) -> c.getStreamId().equals(streamId))
                        .collect(Collectors.toList());                        
                commits.stream().forEach((commit) -> {
                    this.commits.remove(commit);
                });
                List<Snapshot> snapshots = this.snapshots.stream()
                        .filter((s) -> s.getStreamId().equals(streamId))
                        .collect(Collectors.toList());                        
                snapshots.stream().forEach((snapshot) -> {
                    this.snapshots.remove(snapshot);
                });                
                StreamHead streamHead = this.heads.stream()
                        .filter((s) -> s.getStreamId().equals(streamId))
                        .findFirst().orElse(null);                        
                if (streamHead != null)
                    this.heads.remove(streamHead);
            }
        }
    }
}
