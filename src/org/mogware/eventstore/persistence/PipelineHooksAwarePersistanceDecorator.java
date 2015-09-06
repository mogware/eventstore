package org.mogware.eventstore.persistence;

import java.time.LocalDateTime;
import java.util.Iterator;
import org.mogware.eventstore.CommitAttempt;
import org.mogware.eventstore.Checkpoint;
import org.mogware.eventstore.Commit;
import org.mogware.eventstore.PipelineHook;
import org.mogware.eventstore.Snapshot;

public class PipelineHooksAwarePersistanceDecorator implements PersistStreams {
    private final PersistStreams original;
    private final PipelineHook[] pipelineHooks;

    public PipelineHooksAwarePersistanceDecorator(PersistStreams original,
            PipelineHook[] pipelineHooks) {
        if (original == null)
            throw new NullPointerException("original must not be null");
        if (original == null)
            throw new NullPointerException("original must not be null");
        this.original = original;
        this.pipelineHooks = pipelineHooks;
    }

    @Override
    public void initialize() {
        this.original.initialize();
    }

    @Override
    public Iterable<Commit> getFrom(String bucketId, String streamId,
            int minRevision, int maxRevision) {
        return executeHooks(
            this.original.getFrom(bucketId, streamId, minRevision, maxRevision)
        );
    }

    @Override
    public Commit commit(CommitAttempt attempt) {
        return this.original.commit(attempt);
    }

    @Override
    public Snapshot getSnapshot(String bucketId, String streamId,
            int maxRevision) {
        return this.original.getSnapshot(bucketId, streamId, maxRevision);
    }

    @Override
    public boolean addSnapshot(Snapshot snapshot) {
        return this.original.addSnapshot(snapshot);
    }

    @Override
    public Iterable<StreamHead> getStreamsToSnapshot(String bucketId, int maxThreshold) {
        return this.original.getStreamsToSnapshot(bucketId, maxThreshold);
    }

    @Override
    public Iterable<Commit> getFrom(String bucketId, LocalDateTime start) {
        return executeHooks(this.original.getFrom(bucketId, start));
    }

    @Override
    public Iterable<Commit> getFrom(String checkpointToken) {
        return executeHooks(this.original.getFrom(checkpointToken));
    }

    @Override
    public Checkpoint getCheckpoint(String checkpointToken) {
        return this.original.getCheckpoint(checkpointToken);
    }

    @Override
    public Iterable<Commit> getFromTo(String bucketId, LocalDateTime start,
            LocalDateTime end) {
        return executeHooks(this.original.getFromTo(bucketId, start, end));
    }

    @Override
    public void purge() {
        this.original.purge();
        for (PipelineHook pipelineHook : this.pipelineHooks)
            pipelineHook.onPurge(null);
    }

    @Override
    public void purge(String bucketId) {
        this.original.purge(bucketId);
        for (PipelineHook pipelineHook : this.pipelineHooks)
            pipelineHook.onPurge(bucketId);
    }

    @Override
    public void drop() {
        this.original.drop();
    }

    @Override
    public void deleteStream(String bucketId, String streamId) {
        this.original.deleteStream(bucketId, streamId);
    }

    private Iterable<Commit> executeHooks(Iterable<Commit> commits) {
        class StateMachine implements Iterable<Commit>, Iterator<Commit> {
            private final Iterator<Commit> source;

            public StateMachine(Iterable<Commit> commits) {
                this.source = commits.iterator();
            }
            @Override
            public Iterator<Commit> iterator() {
                if (this.state != 0)
                    return new StateMachine(commits);
                this.state = 1;
                return this;
            }

            @Override
            public boolean hasNext() {
                if (! this.nextDefined) {
                    this.hasNext = this.state();
                    this.nextDefined = true;
                }
                return this.hasNext;
            }

            @Override
            public Commit next() {
                if (! this.hasNext())
                    throw new java.util.NoSuchElementException();
                this.nextDefined = false;
                return this.next;
            }

            private boolean state() {
                Commit filtered;

                while (true) switch (this.state) {
                case 0:
                    this.state = 1;
                case 1:
                    if (! this.source.hasNext()) {
                        this.state = 2;
                        break;
                    }
                    filtered = this.source.next();
                    for (PipelineHook hook: pipelineHooks) {
                        if ((filtered = hook.select(filtered)) == null)
                            break;
                    }
                    if (filtered == null)
                        break;
                    this.next = filtered;
                    return true;
                case 2:
                default:
                    return false;
                }
            }

            private int state = 0;
            private boolean hasNext = false;
            private boolean nextDefined = false;
            private Commit next;
        }
        return new StateMachine(commits);
    }
}
