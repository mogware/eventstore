package org.mogware.eventstore;

import org.mogware.eventstore.persistence.PersistStreams;
import org.mogware.eventstore.persistence.PipelineHooksAwarePersistanceDecorator;

public class OptimisticEventStore implements EventStore, CommitEvents {
    private final PersistStreams persistence;
    private final PipelineHook[] pipelineHooks;

    public OptimisticEventStore(PersistStreams persistence,
            PipelineHook[] pipelineHooks) {
        if (persistence == null)
            throw new NullPointerException("persistence must not be null");
        this.pipelineHooks = pipelineHooks != null ?
                pipelineHooks : new PipelineHook[0];
        this.persistence = new PipelineHooksAwarePersistanceDecorator(
                persistence, this.pipelineHooks);
    }

    @Override
    public EventStream createStream(String bucketId, String streamId) {
        return new OptimisticEventStream(bucketId, streamId, this);
    }

    @Override
    public EventStream openStream(String bucketId, String streamId,
            int minRevision, int maxRevision) {
        maxRevision = maxRevision <= 0 ? Integer.MAX_VALUE : maxRevision;
        return new OptimisticEventStream(
                bucketId, streamId, this, minRevision, maxRevision);
    }

    @Override
    public EventStream openStream(Snapshot snapshot, int maxRevision) {
        if (snapshot == null)
            throw new NullPointerException("snapshot must not be null");
        maxRevision = maxRevision <= 0 ? Integer.MAX_VALUE : maxRevision;
        return new OptimisticEventStream(snapshot, this, maxRevision);
    }

    @Override
    public Iterable<Commit> getFrom(String bucketId, String streamId,
            int minRevision, int maxRevision) {
        return this.persistence.getFrom(
                bucketId, streamId, minRevision, maxRevision);
    }

    @Override
    public Commit commit(CommitAttempt attempt) {
        for (PipelineHook hook: this.pipelineHooks) {
            if (hook.preCommit(attempt))
                continue;
            return null;
        }
        Commit commit = this.persistence.commit(attempt);
        for (PipelineHook hook: this.pipelineHooks) {
            hook.postCommit(commit);
        }
        return commit;
    }

    @Override
    public PersistStreams getAdvanced() {
        return this.persistence;
    }
}
