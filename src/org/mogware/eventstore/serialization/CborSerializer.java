package org.mogware.eventstore.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.mogware.system.dif.CborReader;
import org.mogware.system.dif.CborWriter;
import org.mogware.system.dif.Decoder;
import org.mogware.system.dif.Encoder;

public class CborSerializer implements Serialize {

    @Override
    public <T> void serialize(OutputStream output, T graph) {
        try {
            CborWriter writer = new CborWriter(output);
            new Encoder().encode(writer, graph);
            writer.close();
        } catch (IOException ex) {
        }
    }

    @Override
    public <T> T deserialize(InputStream input) {
        try {
            return (T) new Decoder().decode(new CborReader(input));            
        } catch (IOException ex) {
            return null;
        }  
    }
}
