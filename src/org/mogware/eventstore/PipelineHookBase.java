package org.mogware.eventstore;

public abstract class PipelineHookBase implements PipelineHook {
    public Commit select(Commit committed) {
        return committed;
    }

    public boolean preCommit(CommitAttempt attempt) {
        return true;
    }

    public void postCommit(Commit committed) {
    }

    public void onPurge(String bucketId) {
    }

    public void onDeleteStream(String bucketId, String streamId) {
    }
}
