package org.mogware.eventstore.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipSerializer implements Serialize {
    private final Serialize inner;
    
    public GzipSerializer(Serialize inner) {
        this.inner = inner;
    }

    @Override
    public <T> void serialize(OutputStream output, T graph) {
        try {
            this.inner.serialize(new GZIPOutputStream(output), graph);
        } catch (IOException ex) {
        }
    }

    @Override
    public <T> T deserialize(InputStream input) {
        try {
            return inner.deserialize(new GZIPInputStream(input));
        } catch (IOException ex) {
            return null;
        }
    }
}
