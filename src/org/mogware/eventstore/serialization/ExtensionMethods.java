package org.mogware.eventstore.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public final class ExtensionMethods {
    private ExtensionMethods() {
    }

    public static <T> byte[] serialize(Serialize serializer, T graph) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        serializer.serialize(stream, graph);
        return stream.toByteArray();
    }

    public static <T> T deserialize(Serialize serializer, byte[] source) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(source);
        return serializer.deserialize(inputStream);
    }
}
