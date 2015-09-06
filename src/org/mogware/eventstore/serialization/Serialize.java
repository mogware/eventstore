package org.mogware.eventstore.serialization;

import java.io.InputStream;
import java.io.OutputStream;

/**
* Provides the ability to serialize and deserialize an object graph.
* 
* Instances of this class must be designed to be multi-thread safe such that
* they can be shared between threads.
*/

public interface Serialize {
    /**
    * Serializes the object graph provided and writes a serialized
    * representation to the output stream provided.
    */
    <T> void serialize(OutputStream output, T graph);

    /**
    * Deserializes the stream provided and reconstructs the corresponding 
    * object graph.
    */
    <T> T deserialize(InputStream input);
}