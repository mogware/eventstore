package org.mogware.eventstore.persistence;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.mogware.eventstore.Bucket;
import org.mogware.eventstore.Commit;
import org.mogware.eventstore.CommitAttempt;
import org.mogware.eventstore.EventMessage;
import org.mogware.system.Guid;

public abstract class TestCasePersistence {
    protected abstract PersistStreams persistence();

    protected void assertPersistenceCommitHeader() {
        String streamId = Guid.newGuid().toString();
        Map<String, Object> headers = new HashMap<>();
        headers.put("key.1", "value");
        List<EventMessage> events = new ArrayList<>();
        CommitAttempt attempt = new CommitAttempt(
                streamId,
                2,
                Guid.newGuid(),
                1,
                LocalDateTime.now(),
                headers,
                events
        );
        persistence().commit(attempt);
        Commit persisted = StreamSupport.stream(persistence().getFrom(
                Bucket.Default, streamId, Integer.MIN_VALUE, Integer.MAX_VALUE
        ).spliterator(), false).findFirst().orElse(null);
        assertTrue(persisted.getHeaders().keySet().contains("key.1"));
    }

    protected void assertPersistenceCommitEvents() {
        String streamId = Guid.newGuid().toString();
        CommitAttempt attempt = buildAttempt(streamId);
        persistence().commit(attempt);
        Commit persisted = StreamSupport.stream(persistence().getFrom(
                Bucket.Default, streamId, Integer.MIN_VALUE, Integer.MAX_VALUE
        ).spliterator(), false).findFirst().orElse(null);
        assertEquals(persisted.getStreamId(), attempt.getStreamId());
        assertEquals(persisted.getStreamRevision(), attempt.getStreamRevision());
        assertEquals(persisted.getCommitId(), attempt.getCommitId());
        assertEquals(persisted.getCommitSequence(), attempt.getCommitSequence());
        assertEquals(persisted.getHeaders().size(), attempt.getHeaders().size());
        assertEquals(persisted.getEvents().size(), attempt.getEvents().size());
    }

    protected void assertPurgingCommitsInMultipleBuckets() {
        String streamId = Guid.newGuid().toString();
        persistence().commit(buildAttempt(streamId, null, "a"));
        persistence().commit(buildAttempt(streamId, null, "b"));
        persistence().purge();
        assertTrue(!persistence().getFrom("a", LocalDateTime.MIN)
                .iterator().hasNext());
        assertTrue(!persistence().getFrom("b", LocalDateTime.MIN)
                .iterator().hasNext());
    }

    protected void assertPersistenceLargePayload() {
        String streamId = Guid.newGuid().toString();
        Map<String, Object> headers = new HashMap<>();
        List<EventMessage> events = new ArrayList<>();
        events.add(new EventMessage().setBody(createString('a',100000)));
        CommitAttempt attempt = new CommitAttempt(
                streamId,
                2,
                Guid.newGuid(),
                1,
                LocalDateTime.now(),
                headers,
                events
        );
        persistence().commit(attempt);
        Commit persisted = StreamSupport.stream(persistence().getFrom(
                Bucket.Default, streamId, Integer.MIN_VALUE, Integer.MAX_VALUE
        ).spliterator(), false).findFirst().orElse(null);
        assertTrue(persisted.getEvents().get(0)
                .getBody().toString().length() == 100000);
    }

    private static CommitAttempt buildAttempt(String streamId) {
        return buildAttempt(streamId, null);
    }

    private static CommitAttempt buildAttempt(String streamId,
            LocalDateTime now) {
        return buildAttempt(streamId, now, null);

    }

    private static CommitAttempt buildAttempt(String streamId,
            LocalDateTime now, String bucketId) {
        now = now != null ? now : LocalDateTime.now();
        bucketId = bucketId  != null ? bucketId : Bucket.Default;
        Map<String, Object> headers = new HashMap<>();
        headers.put("A header", "A string value");
        headers.put("Another header", 2);
        List<EventMessage> events = new ArrayList<>();
        events.add(new EventMessage().setBody(42));
        events.add(new EventMessage().setBody(44));
        return new CommitAttempt(
                bucketId, streamId, 2, Guid.newGuid(), 1,
                now, headers, events);
    }

    private static String createString(char c, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++)
            sb.append(c);
        return sb.toString();
    }
}
