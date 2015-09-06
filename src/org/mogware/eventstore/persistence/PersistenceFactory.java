package org.mogware.eventstore.persistence;

/**
* Indicates the ability to build a ready-to-use persistence engine.
* 
* Instances of this class must be designed to be multi-thread safe such that
* they can be shared between threads.
*/
public interface PersistenceFactory {
    PersistStreams build();
}
