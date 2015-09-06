package org.mogware.eventstore.persistence;

import java.time.LocalDateTime;
import org.mogware.eventstore.Bucket;
import org.mogware.eventstore.Commit;

public final class PersistStreamsExtensions {
    private PersistStreamsExtensions() {
    }

    public static Iterable<Commit> getFrom(PersistStreams persistStreams,
            LocalDateTime start) {
        return persistStreams.getFrom(Bucket.Default, start);
    }

    public static Iterable<Commit> getFromTo(PersistStreams persistStreams,
            LocalDateTime start, LocalDateTime end) {
        return persistStreams.getFromTo(Bucket.Default, start, end);
    }

    public static void deleteStream(PersistStreams persistStreams,
            String streamId) {
        persistStreams.deleteStream(Bucket.Default, streamId);
    }

    public static Iterable<Commit> getFromStart(PersistStreams persistStreams) {
        return persistStreams.getFrom(null);
    }
}
