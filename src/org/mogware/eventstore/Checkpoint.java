package org.mogware.eventstore;

/**
* Represents a storage level checkpoint to order commits.
*/
public interface Checkpoint extends Comparable<Checkpoint> {
    String getValue();
}
