package org.mogware.eventstore;

/**
* Indicates the ability to commit events and access events to and from a given 
* stream.
* 
* Instances of this class must be designed to be multi-thread safe such that 
* they can be shared between threads.
*/

public interface CommitEvents {
    /**
    * Gets the corresponding commits from the stream indicated starting at the 
    * revision specified until the end of the stream sorted in ascending order
    */
    Iterable<Commit> getFrom(String bucketId, String streamId,
            int minRevision, int maxRevision);
    /**
    * Writes the to-be-commited events provided to the underlying persistence
    * mechanism.
    */
    Commit commit(CommitAttempt attempt);
}
