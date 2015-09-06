package org.mogware.eventstore.serialization;

import org.mogware.eventstore.Wireup;

public class SerializationWireup extends Wireup {

    public SerializationWireup(Wireup inner, Serialize instance) {
        super(inner);
        this.with(Serialize.class, instance);          
    }
    
    public SerializationWireup compress() {
        Serialize wrapped = (Serialize)
                this.getContainer().resolve(Serialize.class);
        this.with(Serialize.class, new GzipSerializer(wrapped));
        return this;
    }
    
    public SerializationWireup encryptWith(byte[] encryptionKey) {
        Serialize wrapped = (Serialize)
                this.getContainer().resolve(Serialize.class);
        this.with(Serialize.class, new AesSerializer(wrapped, encryptionKey));
        return this;
    }    
}
