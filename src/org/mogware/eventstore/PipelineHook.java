package org.mogware.eventstore;

/**
* Provides the ability to hook into the pipeline of persisting a commit.
* 
* Instances of this class must be designed to be multi-thread safe such that
* they can be shared between threads.
*/

public interface PipelineHook {
    /**
    * Hooks into the selection pipeline just prior to the commit being returned
    * to the caller.
    */
    Commit select(Commit committed);

    /**
    * Hooks into the commit pipeline prior to persisting the commit to durable
    * storage.
    */
    boolean preCommit(CommitAttempt attempt);

    /**
    * Hooks into the commit pipeline just after the commit has been 
    * *successfully* committed to durable storage.
    */
    void postCommit(Commit committed);

    /**
    * Invoked when a bucket has been purged. If buckedId is null, then all 
    * buckets have been purged.
    */
    void onPurge(String bucketId);

    /**
    * Invoked when a stream has been deleted.
    */
    void onDeleteStream(String bucketId, String streamId);
}
