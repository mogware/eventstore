package org.mogware.eventstore;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.List;
import org.mogware.system.Guid;

/**
* Represents a series of events which have been fully committed as a single 
* unit and which apply to the stream indicated.
*/

public interface Commit {
    /**
    * Gets the value which identifies bucket to which the stream and the
    * commit belongs.
    */
    String getBucketId();

    /**
    * Gets the value which uniquely identifies the stream to which the commit
    * belongs.
    */
    String getStreamId();
    
    /**
    * Gets the value which indicates the revision of the most recent event in
    * the stream to which this commit applies.
    */
    int getStreamRevision();
    
    /**
    * Gets the value which uniquely identifies the commit within the stream.
    */
    Guid getCommitId();

    /**
    * Gets the value which indicates the sequence (or position) in the stream
    * to which this commit applies.
    */
    int getCommitSequence();

    /**
    * Gets the point in time at which the commit was persisted.
    */
    LocalDateTime getCommitStamp();

    /**
    * Gets the meta data which provides additional, unstructured information
    * about this commit.
    */
    Map<String,Object> getHeaders();
    /**
    * Gets the collection of event messages to be committed as a single unit.
    */
    List<EventMessage> getEvents() ;

    /**
    * The checkpoint that represents the storage level order.
    */
    String getCheckpointToken();
}
