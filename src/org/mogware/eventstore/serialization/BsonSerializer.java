package org.mogware.eventstore.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.mogware.system.dif.BsonReader;
import org.mogware.system.dif.BsonWriter;
import org.mogware.system.dif.Decoder;
import org.mogware.system.dif.Encoder;

public class BsonSerializer implements Serialize {

    @Override
    public <T> void serialize(OutputStream output, T graph) {
        try {
            BsonWriter writer = new BsonWriter(output);
            new Encoder().encode(writer, graph);
            writer.close();
        } catch (IOException ex) {
        }
    }

    @Override
    public <T> T deserialize(InputStream input) {
        try {
            return (T) new Decoder().decode(new BsonReader(input));            
        } catch (IOException ex) {
            return null;
        }  
    }
}
