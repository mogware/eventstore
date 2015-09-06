package org.mogware.eventstore.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.mogware.system.dif.Decoder;
import org.mogware.system.dif.JsonWriter;
import org.mogware.system.dif.Encoder;
import org.mogware.system.dif.JsonReader;

public class JsonSerializer implements Serialize {

    @Override
    public <T> void serialize(OutputStream output, T graph) {
        try {
            JsonWriter writer = new JsonWriter(output);
            new Encoder().encode(writer, graph);
            writer.close();
        } catch (IOException ex) {
        }
    }

    @Override
    public <T> T deserialize(InputStream input) {
        try {
            return (T) new Decoder().decode(new JsonReader(input));            
        } catch (IOException ex) {
            return null;
        }        
    }
}
