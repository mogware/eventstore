package org.mogware.eventstore;

import org.mogware.eventstore.persistence.StreamHead;

/**
* Indicates the ability to get or retrieve a snapshot for a given stream.
* 
* Instances of this class must be designed to be multi-thread safe such that
* they can be shared between threads.
*/
public interface AccessSnapshots {
    /**
    * Gets the most recent snapshot which was taken on or before the revision
    * indicated.
    */
    Snapshot getSnapshot(String bucketId, String streamId, int maxRevision);

    /**
    * Adds the snapshot provided to the stream indicated.
    */
    boolean addSnapshot(Snapshot snapshot);

    /**
    * Gets identifiers for all streams whose head and last snapshot revisions
    * differ by at least the threshold specified.
    */
    Iterable<StreamHead> getStreamsToSnapshot(String bucketId, int maxThreshold);
}
