package org.mogware.eventstore.persistence;

public interface StreamHead {
    /**
    * Gets the value which uniquely identifies the stream where the last
    * snapshot exceeds the allowed threshold.
    */
    String getBucketId();

    /**
    * Gets the value which uniquely identifies the stream where the last
    * snapshot exceeds the allowed threshold.
    */
    String getStreamId();

    /**
    * Gets the value which indicates the revision, length, or number of events
    * committed to the stream.
    */
    int getHeadRevision();

    /**
    * Gets the value which indicates the revision at which the last snapshot
    * was taken.
    */
    int getSnapshotRevision();
    
}
