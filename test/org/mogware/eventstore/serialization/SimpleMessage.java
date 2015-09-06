package org.mogware.eventstore.serialization;

import java.util.ArrayList;
import java.util.List;
import org.mogware.system.Guid;

public class SimpleMessage {
    public Guid id;
    public String value;
    public int count;
    public List<String> contents;

    public SimpleMessage() {
        this.contents = new ArrayList<>();
    }

    public static SimpleMessage populate() {
        SimpleMessage message = new SimpleMessage();
        message.id = Guid.newGuid();
        message.count = 1234;
        message.value = "Hello World!";
        message.contents.add("a");
        message.contents.add(null);
        message.contents.add("");
        message.contents.add("d");
        return message;
    }
}
