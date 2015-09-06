package org.mogware.eventstore.persistence;

public class DefaultStreamHead implements StreamHead {
    public DefaultStreamHead(String bucketId, String streamId, int headRevision,
            int snapshotRevision) {
        this();
        this.bucketId = bucketId;
        this.streamId = streamId;
        this.headRevision = headRevision;
        this.snapshotRevision = snapshotRevision;
    }

    protected DefaultStreamHead() {
    }

    private String bucketId;
    public String getBucketId() {
        return this.bucketId;
    }

    public void setBucketId(String value) {
        this.bucketId = value;
    }

    private String streamId;
    public String getStreamId() {
        return this.streamId;
    }

    public void setStreamId(String value) {
        this.streamId = value;
    }

    private int headRevision;
    public int getHeadRevision() {
        return this.headRevision;
    }

    public void setHeadRevision(int value) {
        this.headRevision = value;
    }

    private int snapshotRevision;
    public int getSnapshotRevision() {
        return this.snapshotRevision;
    }

    public void setSnapshotRevision(int value) {
        this.snapshotRevision = value;
    }

    protected boolean equals(DefaultStreamHead other) {
        return this.streamId.equals(other.streamId);
    }
    
    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null || (this.getClass() != other.getClass()))
            return false;
        return this.equals((DefaultStreamHead) other);
    }

    @Override
    public int hashCode() {
        return this.hashCode();
    }
}
