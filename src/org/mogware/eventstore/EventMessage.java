package org.mogware.eventstore;

import java.util.HashMap;

/**
* Represents a single element in a stream of events.
*/
public class EventMessage {
    private HashMap<String, Object> headers;
    private Object body;
    
    public EventMessage() {
        this.headers = new HashMap<>();
    }

    public HashMap<String, Object> getHeaders() {
        return this.headers;
    }

    public EventMessage setHeaders(HashMap<String, Object> value) {
        this.headers = value; return this;
    }

    public Object getBody() {
        return this.body;
    }

    public EventMessage setBody(Object value) {
        this.body = value; return this;
    }
}
