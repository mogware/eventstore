package org.mogware.eventstore;

public class DefaultSnapshot implements Snapshot {
    private String bucketId;
    private String streamId;
    private int streamRevision;
    private Object payload;    
    
    public DefaultSnapshot(String streamId, int streamRevision, Object payload) {
        this(Bucket.Default, streamId, streamRevision, payload);
    }
    
    public DefaultSnapshot(String bucketId, String streamId, int streamRevision,
            Object payload) {
        this.bucketId = bucketId;
        this.streamId = streamId;
        this.streamRevision = streamRevision;
        this.payload = payload;
    }
    
    @Override
    public String getBucketId() {
        return this.bucketId;
    }

    public void setBucketId(String value) {
        this.bucketId = value;
    }

    @Override
    public String getStreamId() {
        return this.streamId;
    }

   public void setStreamId(String value) {
        this.streamId = value;
    }
   
    @Override
    public int getStreamRevision() {
        return this.streamRevision;
    }

    public void setStreamRevision(int value) {
        this.streamRevision = value;
    }
    
    @Override
    public Object getPayload() {
        return this.payload;
    }

    public void setPayload(Object value) {
        this.payload = value;
    }
}
