package org.mogware.eventstore;

import java.util.List;
import java.util.Map;
import org.mogware.system.Guid;

/**
* Indicates the ability to track a series of events and commit them to durable
* storage.
* 
* Instances of this class are single threaded and should not be shared between
* threads.
*/

public interface EventStream {
    /**
    * Gets the value which uniquely identifies the stream to which the stream
    * belongs.
    */
    String getStreamId();

    /**
    * Gets the value which indicates the most recent committed revision of
    * event stream.
    */
    int getStreamRevision();

    /**
    * Gets the value which indicates the most recent committed sequence
    * identifier of the event stream.
    */
    int getCommitSequence();

    /**
    * Gets the collection of events which have been successfully persisted to
    * durable storage.
    */
    List<EventMessage> getCommittedEvents();

    /**
    * Gets the collection of committed headers associated with the stream.
    */
    Map<String,Object> getCommittedHeaders();

    /**
    * Gets the collection of yet-to-be-committed events that have not yet been
    * persisted to durable storage.
    */
    List<EventMessage> getUncommittedEvents();

    /**
    * Gets the collection of yet-to-be-committed headers associated with the
    * uncommitted events.
    */
    Map<String,Object> getUncommittedHeaders();

    /**
    * Adds the event messages provided to the session to be tracked.
    * 
    *  @param uncommittedEvent The event to be tracked.
    */
    void add(EventMessage uncommittedEvent);

    /**
    * Commits the changes to durable storage.
    */
    void commitChanges(Guid commitId);

    /**
    * Clears the uncommitted changes.
    */
    void clearChanges();
}
