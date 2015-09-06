package org.mogware.eventstore;

import java.text.NumberFormat;

public class LongCheckpoint implements Checkpoint {
    private final long value;
    
    public LongCheckpoint(long value) {
        this.value = value;
    }
    
    @Override
    public String getValue() {
        return NumberFormat.getInstance().format(this.value);
    }

    public long getLongValue() {
        return this.value;
    }
    
    protected int compareTo(LongCheckpoint other) {
        return other.getLongValue() == this.value ? 0 :
                other.getLongValue() < this.value ? 1 : -1;
    }
    
    @Override
    public int compareTo(Checkpoint other) {
        if (other == null)
            return 1;
        if (this.getClass() != other.getClass())
                throw new IllegalStateException("Cannot compare object");
        return this.compareTo((LongCheckpoint) other);
    }
    
    public static LongCheckpoint parse(String checkpointValue) {
        return (checkpointValue == null || checkpointValue.trim().isEmpty()) ? 
                new LongCheckpoint(-1) : 
                new LongCheckpoint(Long.valueOf(checkpointValue));
    }
}
