package org.mogware.eventstore;

/**
* Represents a materialized view of a stream at specific revision.
*/
public interface Snapshot {
    /**
    * Gets the value which uniquely identifies the bucket to which the snapshot
    * applies.
    */
    String getBucketId();

    /**
    * Gets the value which uniquely identifies the stream to which the snapshot
    * applies.
    */
    String getStreamId();

    /**
    * Gets the position at which the snapshot applies.
    */
    int getStreamRevision();

    /**
    * Gets the snapshot or materialized view of the stream at the revision
    * indicated.
    */
    Object getPayload();
}
