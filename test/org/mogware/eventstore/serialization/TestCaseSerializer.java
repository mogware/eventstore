package org.mogware.eventstore.serialization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.mogware.eventstore.DefaultSnapshot;
import org.mogware.eventstore.EventMessage;
import org.mogware.eventstore.Snapshot;
import org.mogware.system.Guid;

public abstract class TestCaseSerializer {
    protected abstract <T> byte[] serialize(T value);
    protected abstract <T> T deserialize(byte[] value);

    protected void assertSerializeSimpleMessage() {
        final SimpleMessage message = SimpleMessage.populate();
        final SimpleMessage deserialized = deserialize(serialize(message));
        assertEquals(deserialized.id, message.id);
        assertEquals(deserialized.value, message.value);
        assertEquals(deserialized.count, message.count);
        assertEquals(deserialized.contents.size(), message.contents.size());
        assertEquals(deserialized.contents, message.contents);
    }

    protected void assertSerializeListOfEventMessages() {
        final List<EventMessage> messages = new ArrayList<>();
        messages.add(new EventMessage().setBody("some value"));
        messages.add(new EventMessage().setBody(42));
        messages.add(new EventMessage().setBody(SimpleMessage.populate()));
        final List<EventMessage> deserialized = deserialize(serialize(messages));
        assertEquals(deserialized.size(), messages.size());
        assertTrue(deserialized.get(2).getBody() instanceof SimpleMessage);
    }

    protected void assertSerializeListOfCommitHeaders() {
        final Map<String, Object> headers = new HashMap<>();
        headers.put("HeadersKey", "SomeValue");
        headers.put("AnotherKey", 42);
        headers.put("AndAnotherKey", Guid.newGuid());
        headers.put("LastKey", SimpleMessage.populate());
        final Map<String, Object> deserialized = deserialize(serialize(headers));
        assertEquals(deserialized.size(), headers.size());
        assertTrue(deserialized.get("LastKey") instanceof SimpleMessage);
    }

    protected void assertSerializePayloadOnSnapshot() {
        final Map<String, List<Integer>> payload = new HashMap<>();
        final Snapshot snapshot = new DefaultSnapshot(
                Guid.newGuid().toString(), 42, payload
        );
        final Snapshot deserialized = deserialize(serialize(snapshot));
        assertEquals(deserialized.getPayload().getClass(),
                snapshot.getPayload().getClass());
        assertEquals(deserialized.getPayload(), snapshot.getPayload());
    }
}
