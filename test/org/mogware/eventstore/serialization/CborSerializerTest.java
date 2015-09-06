package org.mogware.eventstore.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.junit.Test;

public class CborSerializerTest extends TestCaseSerializer {
    private static final Serialize serializer = new CborSerializer();

    @Override
    protected <T> byte[] serialize(T value) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        serializer.serialize(outputStream, value);
        return outputStream.toByteArray();
    }

    @Override
    protected <T> T deserialize(byte[] value) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(value);
        return serializer.deserialize(inputStream);
    }

    @Test
    public void serializeSimpleMessage() {
        System.out.println("CborSerializerTest: serializeSimpleMessage");
        assertSerializeSimpleMessage();
    }

    @Test
    public void serializeListOfEventMessages() {
        System.out.println("CborSerializerTest: serializeListOfEventMessages");
        assertSerializeListOfEventMessages();
    }

    @Test
    public void serializeListOfCommitHeaders() {
        System.out.println("CborSerializerTest: serializeListOfCommitHeaders");
        assertSerializeListOfCommitHeaders();
    }

    @Test
    public void serializePayloadOnSnapshot() {
        System.out.println("CborSerializerTest: serializePayloadOnSnapshot");
        assertSerializePayloadOnSnapshot();
    }
}
