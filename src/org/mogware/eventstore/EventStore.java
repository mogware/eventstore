package org.mogware.eventstore;

import org.mogware.eventstore.persistence.PersistStreams;

/**
* Indicates the ability to store and retrieve a stream of events.
* 
* Instances of this class must be designed to be multi-thread safe such that
* they can be shared between threads.
*/
public interface EventStore {
    /**
    * Gets a reference to the underlying persistence engine which allows direct
    * access to persistence operations.
    */
    PersistStreams getAdvanced();

    /**
    * Creates a new stream.
    */
    EventStream createStream(String bucketId, String streamId);

    /**
    * Reads the stream indicated from the minimum revision specified up to the
    * maximum revision specified or creates
    * an empty stream if no commits are found and a minimum revision of zero is
    * provided.
    */
    EventStream openStream(String bucketId, String streamId, int minRevision,
            int maxRevision);

    /**
    * Reads the stream indicated from the point of the snapshot forward until
    * the maximum revision specified.
    */
    EventStream openStream(Snapshot snapshot, int maxRevision);
}
