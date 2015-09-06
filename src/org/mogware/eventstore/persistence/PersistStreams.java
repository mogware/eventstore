package org.mogware.eventstore.persistence;

/**
* Indicates the ability to adapt the underlying persistence infrastructure to
* behave like a stream of events.
* 
* Instances of this class must be designed to be multi-thread safe such that
* they can be shared between threads.
*/

import java.time.LocalDateTime;
import org.mogware.eventstore.AccessSnapshots;
import org.mogware.eventstore.Checkpoint;
import org.mogware.eventstore.Commit;
import org.mogware.eventstore.CommitEvents;

public interface PersistStreams extends CommitEvents, AccessSnapshots {
    /**
    * Initializes and prepares the storage for use, if not already performed.
    */
    void initialize();
    
    /**
    * Gets all commits on or after from the specified starting time.
    */
    Iterable<Commit> getFrom(String bucketId, LocalDateTime start);
    
    /**
    * Gets all commits after from the specified checkpoint. Use null to get 
    * from the beginning.
    */
    Iterable<Commit> getFrom(String checkpointToken);

    /**
    * Gets a checkpoint object that is comparable with other checkpoints from
    * this storage engine.
    */
    Checkpoint getCheckpoint(String checkpointToken);
    
    /**
    * Gets all commits on or after from the specified starting time and before
    * the specified end time.
    */
    Iterable<Commit> getFromTo(String bucketId,LocalDateTime start,
            LocalDateTime end);

    /**
    * Completely DESTROYS the contents of ANY and ALL streams that have been 
    * successfully persisted.  Use with caution.
    */
    void purge();

    /**
    * Completely DESTROYS the contents of ANY and ALL streams that have been 
    * successfully persisted in the specified bucket.  Use with caution.
    */
    void purge(String bucketId);
    
    /**
    * Completely DESTROYS the contents and schema (if applicable) containing
    * ANY and ALL streams that have been successfully persisted in the 
    * specified bucket.  Use with caution.
    */
    void drop();
    
    /**
    * Deletes a stream.
    */
    void deleteStream(String bucketId, String streamId);    
}
